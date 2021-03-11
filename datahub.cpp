
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
                
                NetworkEventSource() {
                    for (std::size_t i = 0; i < WORKER_THREAD_COUNT; i++) {
                        _kqueues[i] = INVALID_HANDLE;
                    }
                }
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
            std::uint16_t nextBytesToReceive = 0;
            
        private:
            platform::Socket socket = platform::INVALID_SOCKET;
        };
        
        class Node {
            static const std::size_t WORKER_THREAD_COUNT = 2;
            static const std::size_t RECV_BUFFER_SIZE = 65536;
            
        public:
            Node() {}
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

                                        // TODO:
                                        // initial handshake -> client time, key
                                        
                                        if (ConnectionState *userState = _onClientConnected(_target, buffer)) {
                                            userState->socket = client;
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
    const std::uint16_t MSG_HEADER_LENGTH = 7; // msg header [1-type | 4-id | 2-dataLength]
    const std::size_t SRV_MAX_CLIENTS = 1024;
    
    enum class MsgType : std::uint8_t {
        VALUE_CHANGED = 0x10,
        ARRAY_ELEMENT_ADDED = 0x11,
        ARRAY_ELEMENT_REMOVED = 0x12,
    };
    
    struct ConnectionState : public network::ConnectionState {
        ClientId id;
    };
    
    class Scope::Data {
    public:
        AccessType accessType;
        std::mutex mutex;
        
        void addSubscriber(const std::shared_ptr<ConnectionState> &client) {
            _subscribers.emplace(client);
        }

        void removeSubscriber(const std::shared_ptr<ConnectionState> &client) {
            _subscribers.erase(client);
        }

        std::size_t getSubscribersList(ConnectionState *(&states)[SRV_MAX_CLIENTS]) {
            std::size_t count = 0;
            
            for (auto &item : _subscribers) {
                ids[count++] = item;
            }
            
            return count;
        }
        
    private:
        std::unordered_set<std::weak_ptr<ConnectionState>> _subscribers;
    };

    // TODO: mutlithread access
    class DataHub::Data {
    public:
        DataHubType type;
        std::atomic<ElementId> nextElementUID {0x100};
        Scope::Data *rootScopeData;

        std::function<bool(ClientId, const std::uint8_t *)> clientConnected;
        std::function<void(ClientId)> clientDisconnected;
        std::function<void()> disconnected;

        // TODO: return guard-like object (lock on mutex)
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
        
        // TODO: remove elements (array item removed)
        
        bool listen(const char *ip, std::uint16_t portTCP, std::uint16_t portUDP) {
            return _network.listen<Data, &Data::_onClientConnected, &Data::_onClientDisconnected, &Data::_onServerDataReceived, &Data::_onServerError>(this, ip, portTCP, portUDP);
        }
        
        bool connect(const char *ip, std::uint16_t portTCP, std::uint16_t portUDP, const std::uint8_t *key) {
            return _network.connect<Data, &Data::_onDisconnected, &Data::_onClientDataReceived, &Data::_onClientError>(this, ip, portTCP, portUDP, key, MSG_HEADER_LENGTH);
        }

        void sendDataToClients(const std::uint8_t *data, std::uint16_t length, ClientId *ids, std::size_t idCount) {
            for (std::size_t i = 0; i < idCount; i++) {
                auto index = _connections.find(ids[i]);
                if (index != _connections.end()) {
                    _network.sendDataToClient(&index->second, data, length);
                }
            }
        }

        void sendData(const std::uint8_t *data, std::uint16_t length) {
            _network.sendData(data, length);
        }
        
        void disconnectClient(ConnectionState *client) {
            _network.disconnectClient(client);
        }

        void disconnect() {
            _network.disconnect();
        }

    private:
        network::ConnectionState *_onClientConnected(const std::uint8_t *key) {
            std::shared_ptr<ConnectionState> connection = nullptr;

            if (_connections.size() < SRV_MAX_CLIENTS) {
                ClientId id = _nextClientUID++;

                if (clientConnected(id, key)) {
                    connection = std::make_shared<ConnectionState>();
                    rootScopeData->addSubscriber(id); // root scope is observed by all clients
                    connection = &_connections[id];
                    connection->nextBytesToReceive = MSG_HEADER_LENGTH;
                    connection->id = id;
                }
            }
            
            return connection.get();
        }
        
        void _onClientDisconnected(network::ConnectionState *state) {
            ConnectionState *connection = static_cast<ConnectionState *>(state);
            clientDisconnected(connection->id);
            _connections.erase(connection->id);
        }
        
        void _onDisconnected() {
            disconnected();
        }
        
        void _onServerDataReceived(network::ConnectionState *state, const std::uint8_t *input, std::uint16_t length) {
            
        }
                
        void _onClientDataReceived(const std::uint8_t *input, std::uint16_t length) {
            
        }
        
        void _onClientError(const char *error) {
            DataHub::onError("[DataHub Client] %s", error);
        }

        void _onServerError(const char *error) {
            DataHub::onError("[DataHub Server] %s", error);
        }

        network::Node _network;
        std::mutex _connectionsGuard;

        ClientId _nextClientUID = 0x100;
        std::unordered_map<ElementId, Base *> _elements;
        std::unordered_map<ClientId, std::shared_ptr<ConnectionState>> _connections;
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
    
    void Scope::_onValueChanged(std::uint32_t id, const std::uint8_t *valueSource, std::uint16_t length) {
        DataHub::Data &dh = *datahub->_data;
        std::uint8_t (&buffer)[BUFFER_SIZE] = DataHub::_getWorkingBuffer();
        
        if (auto element = dh.findElement(id)) {
            auto bufferRef = std::cref(buffer);
            
            // TODO: skip if no network
            
            *(MsgType *)(buffer + 0) = MsgType::VALUE_CHANGED;
            *(std::uint32_t *)(buffer + 1) = id;
            *(std::uint16_t *)(buffer + 5) = length;
            
            std::memcpy(buffer + MSG_HEADER_LENGTH, valueSource, length);
            
            if (dh.type == DataHubType::CLIENT) {
                dh.sendData(buffer, length);
            }
            else if (dh.type == DataHubType::SERVER) {
                ClientId subscribers[SRV_MAX_CLIENTS];
                std::size_t count = element->scope->_data->getSubscribersList(subscribers);
                dh.sendDataToClients(buffer, length, subscribers, count);
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
        if (_data->type == DataHubType::SERVER) {
            _data->clientConnected = cn;
            _data->clientDisconnected = dn;
            
            if (_data->listen(ip, portTCP, portUDP)) {
                return true;
            }
            else {
                DataHub::onError("At 'DataHub::startServer(...)' : server can't start listening");
            }
        }
        else {
            DataHub::onError("At 'DataHub::startServer(...)' : datahub of type = SERVER is required");
        }
        
        return false;
    }
    
    void DataHub::disconnect(std::uint32_t clientId) {
        if (_data->type == DataHubType::SERVER) {
            _data->disconnectClient(clientId);
        }
        else {
            DataHub::onError("At 'DataHub::disconnect(...)' : datahub of type = SERVER is required");
        }
    }
    
    bool DataHub::connect(const char *ip, std::uint16_t portTCP, std::uint16_t portUDP, const std::uint8_t *key, std::function<void()> &&dn) {
        if (_data->type == DataHubType::CLIENT) {
            _data->disconnected = dn;
            
            if (_data->connect(ip, portTCP, portUDP, key)) {
                return true;
            }
            else {
                DataHub::onError("At 'DataHub::startServer(...)' : client can't connect");
            }
        }
        else {
            DataHub::onError("At 'DataHub::connect(...)' : datahub of type = CLIENT is required");
        }
        
        return false;
    }
    
    void DataHub::disconnect() {
        if (_data->type == DataHubType::CLIENT) {
            _data->disconnect();
        }
        else {
            DataHub::onError("At 'DataHub::disconnect()' : datahub of type = CLIENT is required");
        }
    }

    std::uint8_t (&DataHub::_getWorkingBuffer())[BUFFER_SIZE] {
        static thread_local std::uint8_t workingBuffer[datahub::BUFFER_SIZE];
        return workingBuffer;
    }

    ElementId DataHub::_getNextID() {
        return _data->nextElementUID++;
    }

    bool DataHub::_initializeDatahub(DataHub *dh, std::size_t lsize, DataHubType type) {
        std::uint8_t *dataOffset = reinterpret_cast<std::uint8_t *>(dh) + sizeof(DataHub);
        std::size_t dataLength = lsize;

        if (validateLayout(dataOffset, dataLength)) {
            dh->_data = std::make_unique<DataHub::Data>();
            dh->_data->rootScopeData = dh->_root._data.get();
            dh->_data->type = type;

            if (DataHub::_initializeLayout(&dh->_root, type == DataHubType::SERVER ? AccessType::READWRITE : AccessType::READONLY, dataOffset, dataLength)) {
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





