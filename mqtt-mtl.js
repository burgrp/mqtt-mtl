const mqtt = require("mqtt");

let brokers = {};

module.exports = brokerUrl => {

    if (!brokers[brokerUrl]) {
        let broker = {

            connection: mqtt.connect(brokerUrl),

            listeners: [],
            nextListenerId: 0,
            subscriptions: {},

            publish(topic, message) {
                this.connection.publish(topic, message);
            },

            subscribe(topic, listener) {

                if (!(topic instanceof Object)) {
                    topic = {
                        strict: topic,
                        loose: topic
                    }
                }

                if (!this.subscriptions[topic.loose]) {
                    this.connection.subscribe(topic.loose);
                    this.subscriptions[topic.loose] = 1;
                } else {
                    this.subscriptions[topic.loose]++;
                }

                let parsedTopic = topic.strict.split("/");

                let id = this.nextListenerId++;

                this.listeners[id] = (actTopic, actMessage) => {
                    let parsedActTopic = actTopic.split("/");
                    let matches = true;
                    for (let i in parsedTopic) {
                        if (parsedTopic[i] === parsedActTopic[i] || parsedTopic[i] === "+") {
                            continue;
                        }
                        if (parsedTopic[i] === "#") {
                            break;
                        }
                        matches = false;
                        break;
                    }
                    if (matches) {
                        listener(actTopic, actMessage);
                    }
                };

                return id;
            },

            unsubscribe(listenerId) {
                //TODO: unsubscribe from MQTT if it's last subscription in this.subscriptions[]
                //delete this.listeners[listenerId];
                throw new Error("Not implemented yet");
            }
        }

        brokers[brokerUrl] = broker;

        broker.connection.on("message", (topic, message) => {
            for (listener of Object.values(broker.listeners)) {
                try {
                    listener(topic, message);
                } catch(e) {
                    console.error("Unhandled MQTT listener exception:", e);
                }
            }
        });
    }

    return brokers[brokerUrl];
}