
#include "datahub.hpp"

#include <cstdint>
#include <utility>
#include <thread>
#include <atomic>
#include <vector>
#include <unordered_set>

#include <sys/types.h>
#include <sys/event.h>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

namespace datahub {
    namespace network {
        namespace platform {
            using Socket = int;
            static const Socket INVALID_SOCKET = -1;

            class BufferAllocator {
            public:
                struct Buffer {
                    std::atomic<Buffer *> next;
                    std::size_t length;
                    std::uint8_t data[sizeof(size_t)];
                };

                Buffer *alloc(const void *data, std::size_t length) {
                    std::size_t maxLength = std::max(sizeof(size_t), length);
                    Buffer *result = reinterpret_cast<Buffer *>(::malloc(offsetof(Buffer, data) + maxLength));
                    
                    if (data) {
                        ::memcpy(result->data, data, maxLength);
                    }
                    else {
                        ::memset(result->data, 0, maxLength);
                    }
                    
                    result->length = length;
                    result->next = nullptr;
                    return result;
                }

                void free(Buffer *buffer) {
                    return ::free(buffer);
                }
            };

            class ThreadFactory {
            public:
                ThreadFactory() = default;
                ~ThreadFactory() {
                    for (auto &item : _threads) {
                        item.join();
                    }
                }
                
                template <typename Lambda, void(Lambda::*)() const = &Lambda::operator()> void createThread(Lambda functor) {
                    _threads.emplace_back(std::forward<Lambda>(functor));
                };
                
            private:
                std::vector<std::thread> _threads;
            };

            enum class NetworkEventType {
                DISCONNECTED = 0,
                DATA_RECEIVED = EVFILT_READ,
            };

            template<std::size_t WORKER_THREAD_COUNT = 1> class NetworkEventSource {
            public:
                static const int INVALID_HANDLE = -1;
                
                NetworkEventSource() = default;
                ~NetworkEventSource() {
                    for (std::size_t i = 0; i < WORKER_THREAD_COUNT; i++) {
                        ::close(_kqueues[i]);
                        _kqueues[i] = INVALID_HANDLE;
                    }
                }
                
                void registerSocket(Socket socket, void *userData) {
                    struct kevent event = {};
                    
                    event.ident = socket;
                    event.filter = EVFILT_READ;
                    event.flags = EV_ADD;
                    event.udata = userData;

                    if (_kqueues[0] == INVALID_HANDLE) {
                        for (std::size_t i = 0; i < WORKER_THREAD_COUNT; i++) {
                            _kqueues[i] = ::kqueue();
                        }
                    }

                    ::kevent(_kqueues[_index], &event, 1, nullptr, 0, nullptr);
                                    
                    if (++_index == WORKER_THREAD_COUNT) {
                        _index = 0;
                    }
                }
                
                template <typename Lambda, void(Lambda::*)(NetworkEventType, Socket, std::size_t, void *) const = &Lambda::operator()>
                bool waitNetworkEvent(std::size_t threadIndex, Lambda functor) {
                    if (threadIndex < WORKER_THREAD_COUNT) {
                        struct timespec timeout {};
                        timeout.tv_sec = 1;
                        
                        struct kevent incomingEventList[32];
                        int numEvents = ::kevent(_kqueues[threadIndex], nullptr, 0, incomingEventList, 32, &timeout);
                        
                        if (_kqueues[threadIndex] == -1) {
                            return false;
                        }
                        
                        for (int i = 0; i < numEvents; i++) {
                            int socket = static_cast<int>(incomingEventList[i].ident);
                            
                            if (incomingEventList[i].flags & EV_EOF) {
                                functor(NetworkEventType::DISCONNECTED, socket, 0, incomingEventList[i].udata);
                            }
                            else {
                                functor(NetworkEventType(incomingEventList[i].filter), socket, std::size_t(incomingEventList[i].data), incomingEventList[i].udata);
                            }
                        }

                        return true;
                    }
                    
                    return false;
                }
                
            private:
                std::size_t _index = 0;
                int _kqueues[WORKER_THREAD_COUNT];
            };

            class NetworkAPI {
            public:
                bool createSocket(Socket &socket) {
                    socket = ::socket(AF_INET, SOCK_STREAM, 0);
                
                    if (socket != INVALID_SOCKET) {
                        int opt = 1;
                        ::setsockopt(socket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt));
                        return true;
                    }
                    
                    return false;
                }
                
                bool connect(Socket socket, const char *addr, std::uint16_t portTCP) {
                    struct sockaddr_in serverAddress = {};

                    serverAddress.sin_family = AF_INET;
                    serverAddress.sin_port = htons( portTCP );
                    
                    if (::inet_pton(AF_INET, addr, &serverAddress.sin_addr) > 0) {
                        if (::connect(socket, (struct sockaddr *)&serverAddress, sizeof(serverAddress)) == 0) {
                            return true;
                        }
                    }
                    
                    return false;
                }
                
                bool bindAndListen(Socket socket, const char *addr, std::uint16_t portTCP) {
                    struct sockaddr_in bindAddress = {};

                    bindAddress.sin_family = AF_INET;
                    bindAddress.sin_port = htons( portTCP );
                    
                    if (::inet_pton(AF_INET, addr, &bindAddress.sin_addr) > 0) {
                        if (::bind(socket, (struct sockaddr *)&bindAddress, sizeof(bindAddress)) == 0) {
                            if (::listen(socket, 1000) == 0) {
                                return true;
                            }
                        }
                    }
                    
                    return false;
                }
                
                bool accept(Socket listeningSocket, Socket &client) {
                    struct sockaddr_in address = {};
                    socklen_t addrlen = 0;
                    client = ::accept(listeningSocket, (struct sockaddr *)&address, (socklen_t*)&addrlen);
                    return client != INVALID_SOCKET;
                }
                
                bool configureSocket(Socket socket) {
                    int opt = 1;
                    std::size_t sndBufferLength = 512 * 1024;

                    if (::setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == 0) {
                        if (::setsockopt(socket, SOL_SOCKET, SO_SNDBUF, &sndBufferLength, sizeof(sndBufferLength)) == 0) {
                            return true;
                        }
                    }
                    
                    return false;
                }
                
                void close(Socket sock, bool shutdown) {
                    if (shutdown) {
                        ::shutdown(sock, SHUT_RDWR);
                    }
                    
                    ::close(sock);
                }
                
                std::uint16_t recv(Socket sock, std::uint8_t *buffer, std::uint16_t length) {
                    std::uint16_t total = 0;
                    
                    do {
                        total += ::recv(sock, buffer + total, length - total, 0);
                    }
                    while (total < length);
                    return total;
                }
                
                bool send(Socket sock, const std::uint8_t *buffer, std::uint16_t length) {
                    return ::send(sock, buffer, length, 0);
                }
            };
        }

        struct ConnectionState {
            friend class Node;
            friend class Base;
            friend class Server;
            friend class Client;

            std::uint16_t nextBytesToReceive = 0;
            
        private:
            platform::Socket socket = platform::INVALID_SOCKET;
            platform::BufferAllocator::Buffer * sendQueueHead = nullptr;
            std::atomic<platform::BufferAllocator::Buffer *> sendQueueTail {nullptr};
        };
        
        class Node {
            static const std::size_t WORKER_THREAD_COUNT = 2;
            static const std::size_t RECV_BUFFER_SIZE = 65536;
            
        public:
            Node() {
                
            }
            
            ~Node() {
                if (_listeningSocket != platform::INVALID_SOCKET) {
                    _networkAPI.close(_listeningSocket, true);
                }
                if (_clientState.socket != platform::INVALID_SOCKET) {
                    _networkAPI.close(_clientState.socket, true);
                }
            }

            template <typename T,
                ConnectionState *(T::*onClientConnected)(const std::uint8_t *),
                void (T::*onClientDisconnected)(ConnectionState *),
                void (T::*onDataReceived)(ConnectionState *, const std::uint8_t *, std::uint16_t),
                void (T::*onError)(const char *msg)
            >
            bool listen(T *target, const char *addr, std::uint16_t portTCP, std::uint16_t portUDP) {
                if (_target != nullptr) {
                    return false;
                }

                _target = target;
                _onClientConnected = Method<decltype(onClientConnected)>::template asFunc<onClientConnected>();
                _onClientDisconnected = Method<decltype(onClientDisconnected)>::template asFunc<onClientDisconnected>();
                _onServerDataReceived = Method<decltype(onDataReceived)>::template asFunc<onDataReceived>();
                _onServerError = Method<decltype(onError)>::template asFunc<onError>();
                
                if (_networkAPI.createSocket(_listeningSocket)) {
                    if (_networkAPI.bindAndListen(_listeningSocket, addr, portTCP)) {
                        _threadFactory.createThread([this]() {
                            while (true) {
                                platform::Socket client {};
                                
                                if (_networkAPI.accept(_listeningSocket, client)) {
                                    if (_networkAPI.configureSocket(client)) {
                                        thread_local static std::uint8_t buffer[RECV_BUFFER_SIZE];

                                        // ToDo:
                                        // initial handshake -> client time, key
                                        
                                        if (ConnectionState *userState = _onClientConnected(_target, buffer)) {
                                            platform::BufferAllocator::Buffer *stub = _bufferAllocator.alloc(nullptr, 0);
                                            userState->socket = client;
                                            userState->sendQueueHead = stub;
                                            userState->sendQueueTail = stub;

                                            _serverNetworkEventSource.registerSocket(client, userState);
                                        }
                                        else {
                                            _networkAPI.close(client, true);
                                        }
                                    }
                                    else {
                                        _onServerError(_target, "can't configure socket");
                                        _networkAPI.close(client, true);
                                    }
                                }
                                else {
                                    break;
                                }
                            }
                        });
                            
                        for (std::size_t i = 0; i < WORKER_THREAD_COUNT; i++) {
                            _threadFactory.createThread([this, i]() {
                                thread_local static std::uint8_t buffer[RECV_BUFFER_SIZE];
                                bool again = true;
                                
                                do {
                                    again = _serverNetworkEventSource.waitNetworkEvent(i,
                                        [&](platform::NetworkEventType type, platform::Socket sock, std::size_t dataLength, void *userData) {
                                            ConnectionState *state = reinterpret_cast<ConnectionState *>(userData);
                                            
                                            if (type == platform::NetworkEventType::DATA_RECEIVED) {
                                                std::uint16_t received = state->nextBytesToReceive;
                                                
                                                if (state->nextBytesToReceive <= dataLength) {
                                                    state->nextBytesToReceive = 0;
                                                    _onServerDataReceived(_target, state, buffer, _networkAPI.recv(state->socket, buffer, received));
                                                }
                                            }
                                            else if (type == platform::NetworkEventType::DISCONNECTED) {
                                                _onClientDisconnected(_target, reinterpret_cast<ConnectionState *>(userData));
                                                _networkAPI.close(sock, false);
                                            }
                                        }
                                    );
                                }
                                while(again);
                            });
                        }
                        
                        return true;
                    }
                    else {
                        _onServerError(_target, "can't bind socket");
                    }
                }
                else {
                    _onServerError(_target, "can't create socket to listen");
                }
                
                return false;
            }
            
            template <typename T,
                void (T::*onDisconnected)(),
                void (T::*onDataReceived)(const std::uint8_t *, std::uint16_t),
                void (T::*onError)(const char *msg)
            >
            bool connect(T *target, const char *addr, std::uint16_t portTCP, std::uint16_t portUDP, const std::uint8_t *key, std::uint16_t nextBytesToReceive) {
                if (_target != nullptr) {
                    return false;
                }
                
                _clientState.sendQueueHead = _clientState.sendQueueTail = _bufferAllocator.alloc(nullptr, 0);
                _target = target;
                _onDisconnected = Method<decltype(onDisconnected)>::template asFunc<onDisconnected>();
                _onClientDataReceived = Method<decltype(onDataReceived)>::template asFunc<onDataReceived>();
                _onClientError = Method<decltype(onError)>::template asFunc<onError>();
                
                if (_networkAPI.createSocket(_clientState.socket)) {
                    if (_networkAPI.configureSocket(_clientState.socket)) {
                        if (_networkAPI.connect(_clientState.socket, addr, portTCP)) {
                            // TODO: handshake
                        
                            _clientState.nextBytesToReceive = nextBytesToReceive;
                            _clientNetworkEventSource.registerSocket(_clientState.socket, &_clientState);
                            _threadFactory.createThread([this]() {
                                thread_local static std::uint8_t buffer[RECV_BUFFER_SIZE];
                                bool again = true;
                                
                                do {
                                    again = _clientNetworkEventSource.waitNetworkEvent(0,
                                        [&](platform::NetworkEventType type, platform::Socket sock, std::size_t dataLength, void *userData) {
                                            ConnectionState *state = reinterpret_cast<ConnectionState *>(userData);
                                            
                                            if (type == platform::NetworkEventType::DATA_RECEIVED) {
                                                std::uint16_t received = state->nextBytesToReceive;
                                                
                                                if (state->nextBytesToReceive <= dataLength) {
                                                    state->nextBytesToReceive = 0;
                                                    _onClientDataReceived(_target, buffer, _networkAPI.recv(state->socket, buffer, received));
                                                }
                                            }
                                            else if (type == platform::NetworkEventType::DISCONNECTED) {
                                                _onDisconnected(_target);
                                                _networkAPI.close(sock, false);
                                            }
                                        }
                                    );
                                }
                                while(again);
                            });
                            
                            return true;
                        }
                        else {
                            _onClientError(_target, "unable to connect");
                        }
                    }
                    else {
                        _onClientError(_target, "can't configure socket");
                    }
                }
                else {
                    _onClientError(_target, "can't create socket");
                }
                
                return false;
            }
            
            void disconnectClient(ConnectionState *userState) {
                _networkAPI.close(userState->socket, true);
                userState->socket = platform::INVALID_SOCKET;
            }

            void sendDataToClient(ConnectionState *userState, const std::uint8_t *data, std::uint16_t length) {
                _networkAPI.send(userState->socket, data, length);
            }

            void sendData(const std::uint8_t *data, std::uint16_t length) {
                _networkAPI.send(_clientState.socket, data, length);
            }

            void disconnect() {
                _networkAPI.close(_clientState.socket, true);
                _clientState.socket = platform::INVALID_SOCKET;
            }

        protected:
            platform::NetworkAPI _networkAPI;
            platform::BufferAllocator _bufferAllocator;
            platform::ThreadFactory _threadFactory;
            platform::NetworkEventSource<WORKER_THREAD_COUNT> _serverNetworkEventSource;
            platform::NetworkEventSource<> _clientNetworkEventSource;

            platform::Socket _listeningSocket = platform::INVALID_SOCKET;
            ConnectionState _clientState;
            void *_target = nullptr;
            
            // must fill 'nextBytesToReceive' field of 'userData' to control next notification
            void (*_onServerDataReceived)(void *, ConnectionState *userState, const std::uint8_t *input, std::uint16_t length) = nullptr;
            void (*_onClientDataReceived)(void *, const std::uint8_t *input, std::uint16_t length) = nullptr;
            void (*_onServerError)(void *, const char *msg) = nullptr;
            void (*_onClientError)(void *, const char *msg) = nullptr;

            // must return allocated and initialized ConnectionState object or it's derivent.
            // 'nextBytesToReceive' field is amount of bytes to receive before '_onDataReceived' notification
            ConnectionState *(*_onClientConnected)(void *, const std::uint8_t *) = nullptr;
            void (*_onClientDisconnected)(void *, ConnectionState *) = nullptr;
            void (*_onDisconnected)(void *) = nullptr;
        };
    }
}

namespace datahub {
    const std::uint16_t MSG_HEADER_LENGTH = 1;
    
    enum class MsgHeader : std::uint8_t {
        VALUE_CHANGED = 0x10,
        ARRAY_ELEMENT_ADDED = 0x11,
        ARRAY_ELEMENT_REMOVED = 0x11,
    };
    
    struct Scope::Data {
        AccessType accessType;
        std::unordered_set<ClientId> observers;
        std::mutex mutex;
    };

    struct DataHub::Data {
        struct ConnectionState : public network::ConnectionState {
            ClientId id;
        };

        DataHubType type;
        std::atomic<ElementId> nextElementUID {0x100};

        std::function<bool(ClientId, const std::uint8_t *)> clientConnected;
        std::function<void(ClientId)> clientDisconnected;
        std::function<void()> disconnected;

        network::Node network;

        Base *findElement(ElementId id) {
            auto index = _elements.find(id);
            if (index != _elements.end()) {
                return index->second;
            }
            else {
                DataHub::onError("At 'DataHub::Data::findElement(...)' : element not found");
            }
            
            return nullptr;
        }
        
        bool addElement(ElementId id, Base *element) {
            return _elements.emplace(id, element).second;
        }
        
        ConnectionState *findConnection(ClientId id) {
            auto index = _connections.find(id);
            if (index != _connections.end()) {
                return &index->second;
            }
            else {
                DataHub::onError("At 'ServerData::findConnection(...)' : connection not found");
            }
            
            return nullptr;
        }

        network::ConnectionState *onClientConnected(const std::uint8_t *key) {
            ClientId id = _nextClientUID++;
            ConnectionState *connection = nullptr;
            
            if (clientConnected(id, key)) {
                connection = &_connections[id];
                connection->nextBytesToReceive = MSG_HEADER_LENGTH;
                connection->id = id;
            }
            
            return connection;
        }
        
        void onClientDisconnected(network::ConnectionState *state) {
            ConnectionState *connection = static_cast<ConnectionState *>(state);
            clientDisconnected(connection->id);
            _connections.erase(connection->id);
        }
        
        void onDisconnected() {
        
        }
        
        void onServerDataReceived(network::ConnectionState *state, const std::uint8_t *input, std::uint16_t length) {
            
        }
                
        void onClientDataReceived(const std::uint8_t *input, std::uint16_t length) {
            
        }
        
        void onClientError(const char *error) {
            DataHub::onError("[DataHub Client] %s", error);
        }

        void onServerError(const char *error) {
            DataHub::onError("[DataHub Server] %s", error);
        }
        
    private:
        std::atomic<ClientId> _nextClientUID {0x100};

        std::unordered_map<ElementId, Base *> _elements;
        std::unordered_map<ClientId, ConnectionState> _connections;
    };
}

namespace datahub {
    namespace {
        // TODO: validate for serialized size < std::uint16_t::max()
        bool validateLayout(const std::uint8_t *layoutData, std::size_t layoutLength) {
            const std::uint8_t *layoutOffset = layoutData;
            
            while (layoutOffset < layoutData + layoutLength) {
                const Base *item = reinterpret_cast<const Base *>(layoutOffset);
                
                if (std::memcmp(item->sign, "dbase", SIGN_SIZE) != 0) {
                    DataHub::onError("At 'DataHub::_validateLayout(...)' : bad signature");
                    return false;
                }
                if (item->scope != nullptr || item->uid != 0) {
                    DataHub::onError("At 'DataHub::_validateLayout(...)' : already initialized values");
                    return false;
                }
                if (item->type == ElementType::ARRAY) {
                    if ((*reinterpret_cast<const ValidateArrayFunc *>(layoutOffset + sizeof(Base)))() == false) {
                        DataHub::onError("At 'DataHub::_validateLayout(...)' : array validation failed");
                        return false;
                    }
                }

                layoutOffset += item->totalLength;
            }
            
            if (layoutOffset != layoutData + layoutLength) {
                DataHub::onError("At 'DataHub::_validateLayout(...)' : layout length is incorrect");
                return false;
            }
        
            return true;
        }
    }
    
    Scope::Scope() : datahub(nullptr) {
        _data = std::make_unique<Scope::Data>();
        _data->accessType = AccessType::READONLY;
    }
    
    Scope::Scope(DataHub *dh) : datahub(dh) {
        _data = std::make_unique<Scope::Data>();
        _data->accessType = AccessType::READONLY;
    }
    
    Scope::~Scope() {
    
    }
    
    AccessType Scope::getAccessType() const {
        return _data->accessType;
    }
    
    std::mutex &Scope::getMutex() {
        return _data->mutex;
    }

    void Scope::_onValueChanged(std::uint32_t id, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length) {
        DataHub::Data &dh = *datahub->_data;
        
        if (auto element = dh.findElement(id)) {
            auto bufferRef = std::cref(serializedData);
            
            if (dh.type == DataHubType::CLIENT) {

            }
            else if (dh.type == DataHubType::SERVER) {
                //ServerData *serverData = static_cast<ServerData *>(datahub->_network.get());
            }
            
            element->onItemChanged(element, id, &bufferRef);
        }
        else {
            DataHub::onError("At 'Scope::_onValueChanged(...)' : element not found");
        }
    }
    
    void Scope::_onItemAdded(std::uint32_t arrayId, std::uint32_t itemId, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length) {
        if (auto element = datahub->_data->findElement(arrayId)) {
            auto bufferRef = std::cref(serializedData);
            element->onItemChanged(element, itemId, &bufferRef);
        }
        else {
            DataHub::onError("At 'Scope::_onItemAdded(...)' : element not found");
        }
    }
    
    void Scope::_onItemRemoved(std::uint32_t arrayId, std::uint32_t itemId) {
        if (auto element = datahub->_data->findElement(arrayId)) {
            element->onItemChanged(element, itemId, nullptr);
        }
        else {
            DataHub::onError("At 'Scope::_onItemRemoved(...)' : element not found");
        }
    }
    
    DataHub::DataHub() : _root(this) {
        
    }
    
    DataHub::~DataHub() {
        
    }
    
    bool DataHub::startServer(const char *ip, std::uint16_t portTCP, std::uint16_t portUDP, std::function<bool(ClientId, const std::uint8_t *)> &&cn, std::function<void(ClientId)> &&dn) {
        if (_data->type == DataHubType::LOCAL) {
            _data->type = DataHubType::SERVER;
            _data->clientConnected = cn;
            _data->clientDisconnected = dn;
            
            if (_data->network.listen<Data, &Data::onClientConnected, &Data::onClientDisconnected, &Data::onServerDataReceived, &Data::onServerError>(_data.get(), ip, portTCP, portUDP)) {
                return true;
            }
            else {
                DataHub::onError("At 'DataHub::startServer(...)' : server can't start listening");
            }
        }
        else {
            DataHub::onError("At 'DataHub::startServer(...)' : datahub of type = LOCAL is required to start server");
        }
        
        return false;
    }
    
    void DataHub::disconnect(std::uint32_t clientId) {
        if (_data->type == DataHubType::SERVER) {
            if (Data::ConnectionState *state = _data->findConnection(clientId)) {
                _data->network.disconnectClient(state);
            }
            else {
                DataHub::onError("At 'DataHub::disconnect(...)' : connection not found");
            }
        }
        else {
            DataHub::onError("At 'DataHub::disconnect(...)' : datahub is not server");
        }
    }

    std::uint8_t (&DataHub::_getWorkingBuffer())[BUFFER_SIZE] {
        static thread_local std::uint8_t workingBuffer[datahub::BUFFER_SIZE];
        return workingBuffer;
    }

    ElementId DataHub::_getNextID() {
        return _data->nextElementUID++;
    }

    bool DataHub::_initializeLocal(DataHub *dh, std::size_t lsize) {
        std::uint8_t *dataOffset = reinterpret_cast<std::uint8_t *>(dh) + sizeof(DataHub);
        std::size_t dataLength = lsize;

        if (validateLayout(dataOffset, dataLength)) {
            dh->_data = std::make_unique<DataHub::Data>();
            dh->_data->type = DataHubType::LOCAL;

            if (DataHub::_initializeLayout(&dh->_root, AccessType::READWRITE, dataOffset, dataLength)) {
                return true;
            }
            else {
                DataHub::onError("At 'DataHub::_initializeDataHub(...)' : layout initialization failed");
            }
        }
        else {
            DataHub::onError("At 'DataHub::_initializeDataHub(...)' : layout validation failed");
        }

        return false;
    }

    bool DataHub::_initializeClient(DataHub *dh, std::size_t lsize, const char *ip, std::uint16_t portTCP, std::uint16_t portUDP, const std::uint8_t *key, std::function<void()> &&dn) {
        std::uint8_t *dataOffset = reinterpret_cast<std::uint8_t *>(dh) + sizeof(DataHub);
        std::size_t dataLength = lsize;

        if (validateLayout(dataOffset, dataLength)) {
            dh->_data = std::make_unique<DataHub::Data>();
            dh->_data->type = DataHubType::CLIENT;

            if (DataHub::_initializeLayout(&dh->_root, AccessType::READONLY, dataOffset, dataLength)) {
                Data *data = dh->_data.get();
                
                if (data->network.connect<Data, &Data::onDisconnected, &Data::onClientDataReceived, &Data::onClientError>(data, ip, portTCP, portUDP, key, MSG_HEADER_LENGTH)) {
                    return true;
                }
                else {
                    DataHub::onError("At 'DataHub::_initializeClient(...)' : can't connect");
                }
            }
            else {
                DataHub::onError("At 'DataHub::_initializeDataHub(...)' : layout initialization failed");
            }
        }
        else {
            DataHub::onError("At 'DataHub::_initializeDataHub(...)' : layout validation failed");
        }

        return false;
    }

    bool DataHub::_initializeLayout(Scope *scope, AccessType accessType, std::uint8_t *layoutData, std::size_t layoutLength, BufferRef *serializedData) {
        std::uint8_t *layoutOffset = layoutData;
        const std::uint8_t *serializedOffset = serializedData ? serializedData->get() : nullptr;
        scope->_data->accessType = accessType;
        
        while (layoutOffset < layoutData + layoutLength) {
            datahub::Base *item = reinterpret_cast<datahub::Base *>(layoutOffset);

            item->uid = scope->datahub->_getNextID();
            item->scope = scope;

            if (serializedOffset) {
                std::memcpy(layoutOffset + sizeof(datahub::Base), serializedOffset, item->dataLength);
                serializedOffset += item->dataLength;
            }

            if (scope->datahub->_data->addElement(item->uid, item) == false) {
                DataHub::onError("At 'DataHub::_initializeLayout(...)' : id of new element is already in use");
                return false;
            }

            layoutOffset += item->totalLength;
        }
        
        return true;
    }
    
    std::uint16_t DataHub::_serializeLayout(const std::uint8_t *layout, std::size_t length, std::uint8_t (&output)[BUFFER_SIZE]) {
        const std::uint8_t *layoutOffset = layout;
        std::uint8_t *outputOffset = output;
        
        while (layoutOffset < layout + length) {
            const datahub::Base *item = reinterpret_cast<const datahub::Base *>(layoutOffset);
            std::memcpy(outputOffset, layoutOffset + sizeof(datahub::Base), item->dataLength);
            
            outputOffset += item->dataLength;
            layoutOffset += item->totalLength;
        }
        
        return outputOffset - output;
    }
}





