#pragma once

// ToDo: + custom unordered_map with mutex inside (+insert +erase + foreach) for Event<>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>
#include <unordered_map>
#include <functional>
#include <mutex>

namespace datahub {
    template <class> constexpr int FALSE = 0;
    static const std::size_t BUFFER_SIZE = 65536; // must be equal or greater than data/array serialization that is limited by std::uint16_t
    static const std::size_t SIGN_SIZE = 6;

    class Base;
    class Scope;
    class DataHub;
    
    typedef const std::reference_wrapper<const std::uint8_t[BUFFER_SIZE]> BufferRef;
    typedef bool (*ValidateArrayFunc)();
    typedef void (*ItemChangedFunc)(Base *target, std::uint32_t id, BufferRef *);
    typedef struct {} *Token;

    enum class AccessType : std::size_t
    {
        READONLY,
        READWRITE,
    };

    enum class ElementType : std::uint8_t
    {
        VALUE,
        ARRAY,
        RCALL,
    };

    enum class SyncType : std::uint8_t
    {
        STRICT,
        FAST,
    };

    enum class DataHubType
    {
        LOCAL,
        SERVER,
        CLIENT,
    };

    template<typename T> class Event final {
        static_assert(FALSE<T>, "T must be function signature");
    };
    template<typename... Args> class Event<void(Args...)> final {
        template<typename> friend struct Array;
        template<typename> friend struct Value;
        
    public:
        template<typename L, void(L:: *)(Args...) const = &L::operator()> Token operator+=(L &&lambda) {
            static std::size_t _uniqueid = 0x1;
            Token token = reinterpret_cast<Token>(_uniqueid++);
            return _handlers.emplace_back(std::move(token), std::move(lambda)), token;
        }
        void operator-=(Token id) {
            _handlers.erase(std::remove_if(std::begin(_handlers), std::end(_handlers), [id](const auto &item) {
                return item.first == id;
            }), std::end(_handlers));
        }

    private:
        template <typename... CallArgs> void call(CallArgs &&... args) {
            for (const auto &handler : _handlers) {
                handler.second(std::forward<CallArgs>(args)...);
            }
        }

    private:
        std::vector<std::pair<Token, std::function<void(Args...)>>> _handlers;
    };

    // Unified header for all items in DataHub/Scope
    // const values are assigned by item itself
    // other values are assigned by datahub
    //
    struct Base {
        const std::uint8_t sign[SIGN_SIZE] = "dbase";
        const ElementType type;

        SyncType syncType = SyncType::STRICT;
        std::uint32_t uid = 0;
        
        const std::uint16_t dataLength;
        const std::uint16_t totalLength;
        const ItemChangedFunc onItemChanged;
        
        Scope *scope = nullptr;
        
        Base(ElementType elementType, std::uint16_t dataLen, std::uint16_t totalLen, ItemChangedFunc func)
            : type(elementType)
            , dataLength(dataLen)
            , totalLength(totalLen)
            , onItemChanged(func)
        {}
    };
    
    // Base class for array items.
    // it's main purpose is to keep list of clients that have this item in their DataHub and control/filter notifications
    // it provides a mutex so only one thread should access single scope
    //
    class Scope {
        friend class DataHub;
        template<typename> friend struct Value;
        template<typename> friend struct Array;

    public:
        DataHub *datahub;
        
        AccessType getAccessType() const;
        std::mutex &getMutex();
        
    protected:
        Scope();
        ~Scope();

    private:
        Scope(DataHub *dh);

        void _onValueChanged(std::uint32_t id, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length);
        void _onItemAdded(std::uint32_t arrayId, std::uint32_t itemId, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length);
        void _onItemRemoved(std::uint32_t arrayId, std::uint32_t itemId);
        void _onRemoteCall(std::uint32_t id, const std::uint8_t (&serializedData)[BUFFER_SIZE], std::uint16_t length) {}
    
    public:
        struct Data;
        
    private:
        std::unique_ptr<Data> _data;
        
    private:
        Scope(Scope &&) = delete;
        Scope(const Scope &) = delete;
        Scope& operator =(Scope &&) = delete;
        Scope& operator =(const Scope &) = delete;
    };

    // Base class for the user-defined datahub-structure.
    // It contains map of all elements and root scope
    //
    class DataHub {
        friend class Scope;
        template<typename> friend struct Value;
        template<typename> friend struct Array;
        
        template<typename> friend auto make();
        template<typename> friend auto makeAndListen(const char *, std::uint16_t);
        template<typename> friend auto makeAndConnect(const char *, std::uint16_t);

    public:
        // user must provide implementation
        static void onError(const char *msg, ...);
        
    protected:
        DataHub();
        ~DataHub();
        
    private:
        static bool _initializeDataHub(DataHub *datahub, DataHubType type, std::size_t layoutSize, const char *host = nullptr, std::uint16_t portTCP = 0);
        static bool _initializeLayout(Scope *scope, AccessType accessType, std::uint8_t *layoutData, std::size_t layoutLength, BufferRef *serializedData = nullptr);
        static std::uint16_t _serializeLayout(const std::uint8_t *layout, std::size_t length, std::uint8_t (&output)[BUFFER_SIZE]);
        static std::uint8_t (&_getWorkingBuffer())[BUFFER_SIZE];
        
        bool _getNextID(std::uint32_t &outID);
        
    public:
        struct Data;

    private:
        std::unique_ptr<Data> _data;
        Scope _root;
    };
    
    // []
    template<typename T> class Value {
    private:
        friend struct Asserts;
    
        static_assert(std::is_default_constructible<T>::value, "");
        static_assert(std::is_trivially_copyable<T>::value, "");
        static_assert(std::is_copy_assignable<T>::value, "");
        static_assert(sizeof(T) < std::numeric_limits<std::uint16_t>::max(), "");
        
        static void _onItemChanged(Base *target, std::uint32_t id, BufferRef *data) {
            Value<T> *self = reinterpret_cast<Value<T> *>(target);
            const std::uint8_t (&source)[BUFFER_SIZE] = data->get();
            T newValue = *reinterpret_cast<const T *>(source);
            
            {
                std::lock_guard<std::mutex> guard(self->_base.scope->getMutex());
                self->_data = newValue;
            }
            
            self->onValueChanged.call(newValue);
        }
        
        Base _base;
        
    public:
        Value() : _base(ElementType::VALUE, sizeof(T), sizeof(Value<T>), Value<T>::_onItemChanged) {}
        Value &operator =(const T &value) {
            if (_base.scope == nullptr) { // special case to initialize an array item at the first time
                _data = value;
            }
            else {
                if (_base.scope->getAccessType() == AccessType::READWRITE) {
                    std::uint8_t (&buffer)[BUFFER_SIZE] = DataHub::_getWorkingBuffer();
                    *(reinterpret_cast<T *>(buffer)) = value;
                    _base.scope->_onValueChanged(_base.uid, buffer, sizeof(T));
                }
                else {
                    DataHub::onError("At 'Value &Value<T>::operator =(const T &)' : value is readonly");
                }
            }
            
            return *this;
        }

        operator const T() const {
            if (_base.scope) {
                std::lock_guard<std::mutex> guard(_base.scope->getMutex());
                return _data;
            }
            else {
                DataHub::onError("At 'Value &Value<T>::operator T()' : value is not part of DataHub yet");
                return T{};
            }
        }

    private:
        T _data = {};
        
    public:
        Event<void(const T &)> onValueChanged;
    };
    
    // []
    template<typename T> class Array {
    private:
        friend struct Asserts;
        
        static_assert(std::is_base_of<Scope, T>::value, "");
        static_assert(std::is_default_constructible<T>::value, "");
        static_assert(std::is_polymorphic<T>::value == false, "");
        static_assert(sizeof(T) < std::numeric_limits<std::uint16_t>::max(), "");
        
        static bool _validate() {
            T data;
            const std::uint8_t *layoutData = reinterpret_cast<const std::uint8_t *>(&data);
            const std::uint8_t *layoutOffset = layoutData + sizeof(Scope);

            while (layoutOffset < layoutData + sizeof(T)) {
                const Base *item = reinterpret_cast<const Base *>(layoutOffset);

                if (std::memcmp(item->sign, "dbase", SIGN_SIZE) != 0) {
                    DataHub::onError("At 'Array<T>::_validate(...)' : bad signature");
                    return false;
                }
                if (item->type == ElementType::ARRAY) {
                    if ((*reinterpret_cast<const ValidateArrayFunc *>(layoutOffset + sizeof(Base)))() == false) {
                        DataHub::onError("At 'Array<T>::_validate(...)' : sub array validation failed");
                        return false;
                    }
                }

                layoutOffset += item->totalLength;
            }

            if (layoutOffset != layoutData + sizeof(T)) {
                DataHub::onError("At 'Array<T>::_validate(...)' : layout length is incorrect");
                return false;
            }

            return true;
        }
        
        static void _onItemChanged(Base *target, std::uint32_t id, BufferRef *serializedData) {
            Array<T> *self = reinterpret_cast<Array<T> *>(target);
            Scope *scope = self->_base.scope;
            
            if (serializedData == nullptr) {
                self->onElementRemoving.call(id);
                
                {
                    std::lock_guard<std::mutex> guard(scope->getMutex());
                    self->_data.erase(id);
                }
            }
            else {
                T *addedItem = nullptr;
                
                {
                    std::lock_guard<std::mutex> guard(scope->getMutex());
                    addedItem = &(self->_data[id]);
                    addedItem->datahub = scope->datahub;
                    DataHub::_initializeLayout(addedItem, scope->getAccessType(), reinterpret_cast<std::uint8_t *>(addedItem) + sizeof(Scope), sizeof(T) - sizeof(Scope), serializedData);
                }
                
                self->onElementAdded.call(id, *addedItem);
            }
        }
        
        Base _base;
    
    public:
        Array() : _base(ElementType::ARRAY, 0, sizeof(Array<T>), Array<T>::_onItemChanged), _validateFunction(Array<T>::_validate) {}
        
        template<typename L, void(L:: *)(T &) const = &L::operator()> std::uint32_t add(L &&initializer) {
            std::uint32_t resultId = 0;
            
            if (_base.scope) {
                Scope *scope = _base.scope;
            
                if (scope->getAccessType() == AccessType::READWRITE) {
                    if (scope->datahub->_getNextID(resultId)) {
                        T item {};
                        initializer(item);
                        
                        std::uint8_t (&buffer)[BUFFER_SIZE] = DataHub::_getWorkingBuffer();
                        std::uint16_t lengthInBytes = DataHub::_serializeLayout(reinterpret_cast<std::uint8_t *>(&item) + sizeof(Scope), sizeof(T) - sizeof(Scope), buffer);
                    
                        _base.scope->_onItemAdded(_base.uid, resultId, buffer, lengthInBytes);
                    }
                    else {
                        DataHub::onError("At 'Array::add(...)' : cannot generate id");
                    }
                }
                else {
                    DataHub::onError("At 'Array::add(...)' : array is readonly");
                }
            }
            else {
                DataHub::onError("At 'Array<T>::add(...)' : array isn't part of datahub yet");
            }
            
            return resultId;
        }

        void remove(std::uint32_t id) {
            if (_base.scope) {
                Scope *scope = _base.scope;

                if (scope->getAccessType() == AccessType::READWRITE) {
                    scope->_onItemRemoved(_base.uid, id);
                }
                else {
                    DataHub::onError("At 'Array<T>::remove(...)' : array is readonly");
                }
            }
            else {
                DataHub::onError("At 'Array<T>::remove(...)' : array isn't part of datahub yet");
            }
        }
        
        T *operator[](std::uint32_t id) {
            if (_base.scope) {
                std::lock_guard<std::mutex> guard(_base.scope->getMutex());
                
                auto index = _data.find(id);
                if (index != _data.end()) {
                    return &index->second;
                }
            }
            else {
                DataHub::onError("At 'Array<T>::operator[]' : array isn't part of datahub yet");
            }

            return nullptr;
        }

    private:
        const ValidateArrayFunc _validateFunction;
        std::unordered_map<std::uint32_t, T> _data;

    public:
        Event<void(std::uint32_t, T &)> onElementAdded;
        Event<void(std::uint32_t)> onElementRemoving;
    };
    
    // []
    template<typename... Args> class RCall : private Base {
    private:
        static void _onItemChanged(Base *target, std::uint32_t id, BufferRef *serializedData) {
            RCall<Args...> *self = static_cast<RCall<Args...> *>(target);
    
        }
    
    public:
        void call(Args... args) {
        
        }

    public:
        Event<void(Args...)> onTriggered;
    };
    
    struct Asserts {
        struct DataHubLayoutTest : public DataHub {
            Value<std::uint8_t> field;
        };
        struct ScopeLayoutTest : public Scope {
            Value<std::uint8_t> field;
        };

        static_assert(sizeof(DataHub) == offsetof(DataHubLayoutTest, field), "");
        static_assert(sizeof(Scope) == offsetof(ScopeLayoutTest, field), "");
        static_assert(offsetof(Value<std::uint8_t>, _base) == 0, "");
        static_assert(offsetof(Array<ScopeLayoutTest>, _base) == 0, "");
        static_assert(sizeof(Base) == offsetof(Value<std::uint8_t>, _data), "");
        static_assert(sizeof(Base) == offsetof(Array<ScopeLayoutTest>, _validateFunction), "");
        static_assert(sizeof(Base) == 16 + 2 * sizeof(void *), "");
    };
    
    // Make local datahub
    //
    template<typename T> inline auto make() {
        static_assert(std::is_base_of<DataHub, T>::value, "");
        static_assert(std::is_default_constructible<T>::value, "");
        static_assert(std::is_polymorphic<T>::value == false, "");
        
        std::shared_ptr<T> result = std::make_shared<T>();
        
        if (DataHub::_initializeDataHub(result.get(), DataHubType::LOCAL, sizeof(T) - sizeof(DataHub))) {
            return result;
        }
        else {
            DataHub::onError("At 'DataHub::make(...)' : cannot initialize local datahub");
        }
        
        return std::shared_ptr<T>(nullptr);
    }
    
    // Make server datahub
    //
    template<typename T> inline auto makeAndListen(const char *ip, std::uint16_t portTCP) {
        static_assert(std::is_base_of<DataHub, T>::value, "");
        static_assert(std::is_default_constructible<T>::value, "");
        static_assert(std::is_polymorphic<T>::value == false, "");
        
        std::shared_ptr<T> result = std::make_shared<T>();
        
        if (DataHub::_initializeDataHub(result.get(), DataHubType::SERVER, sizeof(T) - sizeof(DataHub), ip, portTCP)) {
            return result;
        }
        else {
            DataHub::onError("At 'DataHub::makeAndListen(...)' : cannot initialize datahub server");
        }
        
        return std::shared_ptr<T>(nullptr);
    };

    // Make client datahub
    //
    template<typename T> inline auto makeAndConnect(const char *server, std::uint16_t portTCP) {
        static_assert(std::is_base_of<DataHub, T>::value, "");
        static_assert(std::is_default_constructible<T>::value, "");
        static_assert(std::is_polymorphic<T>::value == false, "");
        
        std::shared_ptr<T> result = std::make_shared<T>();
        
        if (DataHub::_initializeDataHub(result.get(), DataHubType::CLIENT, sizeof(T) - sizeof(DataHub), server, portTCP)) {
            return result;
        }
        else {
            DataHub::onError("At 'DataHub::makeAndConnect(...)' : cannot initialize datahub client");
        }
        
        return std::shared_ptr<T>(nullptr);
    };

}
