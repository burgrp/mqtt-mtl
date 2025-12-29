const mqtt = require("mqtt");

let brokers = {};

function matchesTopicWildcards(topic, filter) {
    let topicParts = topic.split("/");
    let filterParts = filter.split("/");

    for (let i = 0; i < filterParts.length; i++) {
        let filterPart = filterParts[i];
        let topicPart = topicParts[i];

        if (filterPart === "#") {
            return true;
        } else if (filterPart === "+") {
            continue;
        } else if (filterPart !== topicPart) {
            return false;
        }
    }

    return topicParts.length === filterParts.length;
}

module.exports = brokerUrl => {

    if (!brokers[brokerUrl]) {
        let broker = {

            connection: mqtt.connect(brokerUrl, {
                protocolVersion: 5
            }),

            listeners: [],

            publish(topic, message) {
                this.connection.publish(topic, message);
            },

            subscribe(topic, listener) {
                this.listeners.push({topic, listener});
                this.connection.subscribe(topic);
            },

            unsubscribe(listener) {
                //TODO: unsubscribe from MQTT if it's last subscription in this.subscriptions[]
                //delete this.listeners[listenerId];
                throw new Error("Not implemented yet");
            }
        }

        brokers[brokerUrl] = broker;

        broker.connection.on("message", (topic, message, packet) => {
            for (let {topic: filter, listener} of broker.listeners) {
                if (matchesTopicWildcards(topic, filter)) {
                    try {
                        listener(topic, message.toString());
                    } catch (e) {
                        console.error("Unhandled MQTT listener exception:", e);
                    }
                }
            }
        });
    }

    return brokers[brokerUrl];
}