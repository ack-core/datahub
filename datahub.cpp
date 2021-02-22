
#include "datahub.hpp"

#include <cstdint>
#include <utility>
#include <thread>
#include <atomic>
#include <vector>

#include <sys/types.h>
#include <sys/event.h>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

namespace datahub {
    namespace network {
        template <typename M> struct Method final {};
        template <typename R, typename C, typename... Args> struct Method<R(C::*)(Args...)> final {
            template <R(C::*m)(Args...)> static auto asFunc() {
                struct fn {
                    static R function(void *target, Args... args) {
                        return (reinterpret_cast<C *>(target)->*m)(std::forward<Args>(args)...);
                    }
                };
                return fn::function;
            }
        };
        
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

            class NetworkEventSource {
            public:
                enum class EventType {
                    DISCONNECTED = 0,
                    DATA_RECEIVED = EVFILT_READ,
                };
                
                NetworkEventSource(std::size_t threadCount) {
                    _kqueues.resize(threadCount);
                    
                    for (std::size_t i = 0; i < threadCount; i++) {
                        _kqueues[i] = ::kqueue();
                    }
                }
                
                ~NetworkEventSource() {
                    for (std::size_t i = 0; i < _kqueues.size(); i++) {
                        ::close(_kqueues[i]);
                        _kqueues[i] = -1;
                    }
                }

                void registerSocket(Socket socket, void *userData) {
                    struct kevent event = {};
                    
                    event.ident = socket;
                    event.filter = EVFILT_READ;
                    event.flags = EV_ADD;
                    event.udata = userData;
                    
                    ::kevent(_kqueues[_index], &event, 1, nullptr, 0, nullptr);
                                    
                    if (++_index == _kqueues.size()) {
                        _index = 0;
                    }
                }
                
                template <typename Lambda, void(Lambda::*)(EventType, Socket, std::size_t, void *) const = &Lambda::operator()>
                bool waitNetworkEvent(std::size_t threadIndex, Lambda functor) {
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
                            functor(EventType::DISCONNECTED, socket, 0, incomingEventList[i].udata);
                        }
                        else {
                            functor(EventType(incomingEventList[i].filter), socket, std::size_t(incomingEventList[i].data), incomingEventList[i].udata);
                        }
                    }
                    
                    return true;
                }
                
            private:
                std::size_t _index = 0;
                std::vector<int> _kqueues;
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
            friend class Base;
            friend class Server;
            friend class Client;

            std::uint16_t nextBytesToReceive = 0;
            
        private:
            platform::Socket socket = platform::INVALID_SOCKET;
            platform::BufferAllocator::Buffer * sendQueueHead;
            std::atomic<platform::BufferAllocator::Buffer *> sendQueueTail;
        };

        class Base {
        public:
            Base(std::size_t threadCount) : _networkEventSource(threadCount) {}
        
        protected:
            bool _process(std::uint8_t *workingBuffer, platform::NetworkEventSource::EventType type, platform::Socket sock, std::size_t dataLength, void *userData) {
                ConnectionState *state = reinterpret_cast<ConnectionState *>(userData);
                
                if (type == platform::NetworkEventSource::EventType::DATA_RECEIVED) {
                    std::uint16_t received = state->nextBytesToReceive;
                    
                    if (state->nextBytesToReceive <= dataLength) {
                        state->nextBytesToReceive = 0;
                        _onDataReceived(_target, state, workingBuffer, _networkAPI.recv(state->socket, workingBuffer, received));
                    }
                }
                else if (type == platform::NetworkEventSource::EventType::DISCONNECTED) {
                    return false;
                }
                
                return true;
            }
            
            void _enqueueToSend(ConnectionState *state, const std::uint8_t *data, std::uint16_t length) {
                _networkAPI.send(state->socket, data, length);
            }
            
            platform::NetworkAPI _networkAPI;
            platform::BufferAllocator _bufferAllocator;
            platform::ThreadFactory _threadFactory;
            platform::NetworkEventSource _networkEventSource;
            
            void *_target = nullptr;
            
            // must fill 'nextBytesToReceive' field of 'userData' to control next notification
            void (*_onDataReceived)(void *, ConnectionState *userState, const std::uint8_t *input, std::uint16_t length) = nullptr;
            void (*_onError)(void *, const char *msg) = nullptr;
        };
        
        class Server final : protected Base {
            static const std::size_t WORKER_THREADS = 4;
            static const std::size_t RECV_BUFFER_SIZE = 65536;

        public:
            Server() : Base(WORKER_THREADS) {}
            ~Server() {
                _networkAPI.close(_listeningSocket, true);
            }

            template <typename T,
                ConnectionState *(T::*onClientConnected)(),
                void (T::*onClientDisconnected)(ConnectionState *),
                void (T::*onDataReceived)(ConnectionState *, const std::uint8_t *, std::uint16_t),
                void (T::*onError)(const char *msg)
            >
            void initializeObserver(T *target) {
                _target = target;
                _onClientConnected = Method<decltype(onClientConnected)>::template asFunc<onClientConnected>();
                _onClientDisconnected = Method<decltype(onClientDisconnected)>::template asFunc<onClientDisconnected>();
                _onDataReceived = Method<decltype(onDataReceived)>::template asFunc<onDataReceived>();
                _onError = Method<decltype(onError)>::template asFunc<onError>();
            }
            
            bool listen(const char *addr, std::uint16_t portTCP) {
                if (_target == nullptr) {
                    return false;
                }
                
                if (_networkAPI.createSocket(_listeningSocket)) {
                    if (_networkAPI.bindAndListen(_listeningSocket, addr, portTCP)) {
                        _threadFactory.createThread([this]() {
                            while (true) {
                                platform::Socket client {};
                                
                                if (_networkAPI.accept(_listeningSocket, client)) {
                                    if (_networkAPI.configureSocket(client)) {
                                        if (ConnectionState *userState = _onClientConnected(_target)) {
                                            platform::BufferAllocator::Buffer *stub = _bufferAllocator.alloc(nullptr, 0);
                                            userState->socket = client;
                                            userState->sendQueueHead = stub;
                                            userState->sendQueueTail = stub;

                                            _networkEventSource.registerSocket(client, userState);
                                        }
                                        else {
                                            _networkAPI.close(client, true);
                                        }
                                    }
                                    else {
                                        _onError(_target, "can't configure socket");
                                        _networkAPI.close(client, true);
                                    }
                                }
                                else {
                                    break;
                                }
                            }
                        });
                            
                        for (std::size_t i = 0; i < WORKER_THREADS; i++) {
                            _threadFactory.createThread([this, i]() {
                                thread_local static std::uint8_t buffer[RECV_BUFFER_SIZE];
                                bool again = true;
                                
                                do {
                                    again = _networkEventSource.waitNetworkEvent(i,
                                        [&](platform::NetworkEventSource::EventType type, platform::Socket sock, std::size_t dataLength, void *userData) {
                                            if (_process(buffer, type, sock, dataLength, userData) == false) {
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
                        _onError(_target, "can't bind socket");
                    }
                }
                else {
                    _onError(_target, "can't create socket to listen");
                }
                
                return false;
            }
            
            void disconnect(ConnectionState *userState) {
                _networkAPI.close(userState->socket, true);
                userState->socket = platform::INVALID_SOCKET;
            }
            
            void sendData(ConnectionState *userState, const std::uint8_t *data, std::uint16_t length) {
                _enqueueToSend(userState, data, length);
            }

        private:
            // must return allocated and initialized ConnectionState object or it's derivent.
            // 'nextBytesToReceive' field is amount of bytes to receive before '_onDataReceived' notification
            ConnectionState *(*_onClientConnected)(void *) = nullptr;
            void (*_onClientDisconnected)(void *, ConnectionState *userState) = nullptr;
            platform::Socket _listeningSocket = platform::INVALID_SOCKET;
            
        private:
            Server(Server&&) = delete;
            Server(const Server&) = delete;
            Server& operator =(Server&&) = delete;
            Server& operator =(const Server&) = delete;
        };

        class Client final : protected Base {
            static const std::size_t RECV_BUFFER_SIZE = 65536;

        public:
            Client() : Base(1) {
                _state.sendQueueHead = _state.sendQueueTail = _bufferAllocator.alloc(nullptr, 0);
            }
            ~Client() {
                _networkAPI.close(_state.socket, true);
            }

            template <typename T,
                void (T::*onDisconnected)(),
                void (T::*onDataReceived)(ConnectionState *, const std::uint8_t *, std::uint16_t),
                void (T::*onError)(const char *msg)
            >
            void initializeObserver(T *target) {
                _target = target;
                _onDisconnected = Method<decltype(onDisconnected)>::template asFunc<onDisconnected>();
                _onDataReceived = Method<decltype(onDataReceived)>::template asFunc<onDataReceived>();
                _onError = Method<decltype(onError)>::template asFunc<onError>();
            }
            
            bool connect(const char *addr, std::uint16_t portTCP, std::uint16_t nextBytesToReceive) {
                if (_target == nullptr) {
                    return false;
                }
                
                if (_networkAPI.createSocket(_state.socket)) {
                    if (_networkAPI.configureSocket(_state.socket)) {
                        if (_networkAPI.connect(_state.socket, addr, portTCP)) {
                            _state.nextBytesToReceive = nextBytesToReceive;
                            _networkEventSource.registerSocket(_state.socket, &_state);
                            _threadFactory.createThread([this]() {
                                thread_local static std::uint8_t buffer[RECV_BUFFER_SIZE];
                                bool again = true;
                                
                                do {
                                    again = _networkEventSource.waitNetworkEvent(0,
                                        [&](platform::NetworkEventSource::EventType type, platform::Socket sock, std::size_t dataLength, void *userData) {
                                            if (_process(buffer, type, sock, dataLength, userData) == false) {
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
                            _onError(_target, "unable to connect");
                        }
                    }
                    else {
                        _onError(_target, "can't configure socket");
                    }
                }
                else {
                    _onError(_target, "can't create socket");
                }
                
                return false;
            }
            
            void disconnect() {
                _networkAPI.close(_state.socket, true);
                _state.socket = platform::INVALID_SOCKET;
            }
            
            void sendData(const std::uint8_t *data, std::uint16_t length) { // todo: wouldblock + ev_clear
                _enqueueToSend(&_state, data, length);
            }
            
        private:
            void (*_onDisconnected)(void *) = nullptr;
            ConnectionState _state;
            
        private:
            Client(Client&&) = delete;
            Client(const Client&) = delete;
            Client& operator =(Client&&) = delete;
            Client& operator =(const Client&) = delete;
        };
    }
}

namespace datahub {
    namespace {
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

    struct Scope::Data {
        std::mutex mutex;
        AccessType accessType;
    };
    
    struct DataHub::Data {
        std::atomic<std::uint32_t> nextUID {0x100};
        DataHubType type;
        
        Base *findElement(std::uint32_t id) {
            auto index = _elements.find(id);
            if (index != _elements.end()) {
                return index->second;
            }
            else {
                DataHub::onError("At 'DataHub::Data::findElement(...)' : element not found");
            }
            
            return nullptr;
        }
        
        bool addElement(std::uint32_t id, Base *element) {
            return _elements.emplace(id, element).second;
        }

    private:
        std::unordered_map<std::uint32_t, Base *> _elements;
    };

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

    void Scope::_onValueChanged(std::uint32_t id, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length) { // length is used to send over network
        if (auto element = datahub->_data->findElement(id)) {
            auto bufferRef = std::cref(serializedData);
            element->onItemChanged(element, id, &bufferRef);
        }
    }
    
    void Scope::_onItemAdded(std::uint32_t arrayId, std::uint32_t itemId, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length) {
        if (auto element = datahub->_data->findElement(arrayId)) {
            auto bufferRef = std::cref(serializedData);
            element->onItemChanged(element, itemId, &bufferRef);

            // here we must decide either to send over network or not

        }
    }
    
    void Scope::_onItemRemoved(std::uint32_t arrayId, std::uint32_t itemId) {
        if (auto element = datahub->_data->findElement(arrayId)) {
            element->onItemChanged(element, itemId, nullptr);
        }
    }
    
    struct ServerData : DataHub::Data {
        network::Server network;

        network::ConnectionState *onConnected() {
            
            return nullptr;
        }
        
        void onDisconnected(network::ConnectionState *state) {

        }
        
        void onDataReceived(network::ConnectionState *state, const std::uint8_t *input, std::uint16_t length) {
            
        }

        void onError(const char *error) {
            
        }
    };
    
    struct ClientData : DataHub::Data {
        network::Client network;
        

    };

    DataHub::DataHub() : _root(this) {
        
    }
    
    DataHub::~DataHub() {
    
    }

    std::uint8_t (&DataHub::_getWorkingBuffer())[BUFFER_SIZE] {
        static thread_local std::uint8_t workingBuffer[datahub::BUFFER_SIZE];
        return workingBuffer;
    }

    bool DataHub::_getNextID(std::uint32_t &outID) {
        if (_data->type != DataHubType::CLIENT) {
            outID = _data->nextUID++;
            return true;
        }
        else {
            DataHub::onError("At 'DataHub::_getWorkingBuffer()' : attempt to generate unique id from client");
        }

        return false;
    }

    bool DataHub::_initializeDataHub(DataHub *datahub, DataHubType type, std::size_t layoutSize, const char *host, std::uint16_t portTCP) {
        std::uint8_t *dataOffset = reinterpret_cast<std::uint8_t *>(datahub) + sizeof(DataHub);
        std::size_t dataLength = layoutSize;

        if (validateLayout(dataOffset, dataLength)) {
            if (type == DataHubType::LOCAL) {
                datahub->_data = std::make_unique<DataHub::Data>();
                datahub->_data->type = type;

                if (DataHub::_initializeLayout(&datahub->_root, AccessType::READWRITE, dataOffset, dataLength)) {
                    return true;
                }
            }
            else if (type == DataHubType::SERVER) {
                datahub->_data = std::make_unique<ServerData>();
                datahub->_data->type = type;

                if (DataHub::_initializeLayout(&datahub->_root, AccessType::READWRITE, dataOffset, dataLength)) {
                    ServerData *data = static_cast<ServerData *>(datahub->_data.get());
                    data->network.initializeObserver<ServerData, &ServerData::onConnected, &ServerData::onDisconnected, &ServerData::onDataReceived, &ServerData::onError>(data);
                    
                    if (data->network.listen(host, portTCP)) {
                        return true;
                    }
                    else {
                        DataHub::onError("At 'DataHub::_initializeDataHub(...)' : server can't start listening");
                    }
                }
            }
            else if (type == DataHubType::CLIENT) {
            
            }
            
            DataHub::onError("At 'DataHub::_initializeDataHub(...)' : layout initialization failed");
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

            if (scope->datahub->_getNextID(item->uid)) {
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
            else {
                DataHub::onError("At 'DataHub::_initializeLayout(...)' : cannot generate id");
                return false;
            }
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





