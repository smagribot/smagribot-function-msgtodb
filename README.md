# Smagribot 🌱

Smagribot 🌱 is an open source indoor iot plant sensor for hydroponics.

It takes control of the environmental circumstances for growing vegetables and herbs in your own home. It monitors, among other things, the temperature, humidity and water temperature. The monitored data allows the system to control a grow light and a water pump to optimize the health and growth of the plants from sowing to harvest.

# MessageToDB - Function
Transforms device message to SQL database entry.

## Setup
- Azure IoT Hub endpoint needs `todbfunction`consumer group
- Database must be initialized with `SensorMessages` table

## Enviroment
- `EventHubConnectionAppSetting`: Connection string to built-in Azure IoT Hub eventhub endpoint
- `DBConnection`: Connection string to database