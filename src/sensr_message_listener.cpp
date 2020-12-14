#include "sensr_message_listener.h"
#include <iostream>

namespace sensr {
    MessageListener::MessageListener(ListeningType listening_type) 
    : listening_type_(listening_type) {

    }
    
    void MessageListener::OnError(MessageListener::Error error, const std::string& reason) {
        switch (error) {
            case Error::kConnection: {
                std::cerr << "Lost SENSR Connection fail(Reason: " << reason << "). Please reconnect." << std::endl; 
                break;
            }
            default:
                break;
        }
    }

    inline bool MessageListener::IsOutputMessageListening() const {
        return static_cast<uint32_t>(listening_type_) & static_cast<uint32_t>(ListeningType::kOutputMessage);
    }

    inline bool MessageListener::IsPointResultListening() const {
        return static_cast<uint32_t>(listening_type_) & static_cast<uint32_t>(ListeningType::kPointResult);
    }
}