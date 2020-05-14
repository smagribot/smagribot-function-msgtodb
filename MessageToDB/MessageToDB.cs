using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;

namespace Smagribot.Function
{
    public static class MessageToDB
    {
        private const string InsertMessageSql = "INSERT INTO dbo.SensorMessages (DeviceId, Time, Fanspeed, Temp, Humidity, WaterTemp, Fill) VALUES (@deviceId, @enqueuedTimeUtc, @fanspeed, @temp, @humidity, @waterTemp, @fill);";

        public class DeviceMessage
        {
            public int Fanspeed {get;set;}
            public bool Fill {get;set;}
            public float Temp {get;set;}
            public float Humidity {get;set;}
            public float WaterTemp {get;set;}
        }

        [FunctionName("MessageToDB")]
        public static async Task Run([EventHubTrigger("events/messages", Connection = "EventHubConnectionAppSetting", ConsumerGroup = "todbfunction")] EventData[] events, ILogger log)
        {
            var str = Environment.GetEnvironmentVariable("DBConnection");

            using (var conn = new SqlConnection(str))
            {
                conn.Open();
                
                var exceptions = new List<Exception>();

                foreach (EventData eventData in events)
                {
                    try
                    {
                        string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                        log.LogInformation($"Processing message: {messageBody}");

                        var deviceMsg = JsonConvert.DeserializeObject<DeviceMessage>(messageBody);
                        var deviceId = eventData.SystemProperties["iothub-connection-device-id"];
                        var enqueuedTimeUtc = eventData.SystemProperties.EnqueuedTimeUtc;

                        log.LogInformation($"Properties:\n{string.Join("\n", eventData.Properties.Select(x => $"Property Key: {x.Key} - Data: {x.Value}"))}");
                        log.LogInformation($"SystemProperties:\n{string.Join("\n", eventData.SystemProperties.Select(x => $"Property Key: {x.Key} - Data: {x.Value}"))}");

                        using (var cmd = new SqlCommand(InsertMessageSql, conn))
                        {
                            cmd.Parameters.Add("@deviceId", System.Data.SqlDbType.NVarChar).Value = deviceId;
                            cmd.Parameters.Add("@enqueuedTimeUtc", System.Data.SqlDbType.DateTime).Value = enqueuedTimeUtc;
                            cmd.Parameters.Add("@fanspeed", System.Data.SqlDbType.Int).Value = deviceMsg.Fanspeed;
                            cmd.Parameters.Add("@temp", System.Data.SqlDbType.Float).Value = deviceMsg.Temp;
                            cmd.Parameters.Add("@humidity", System.Data.SqlDbType.Float).Value = deviceMsg.Humidity;
                            cmd.Parameters.Add("@waterTemp", System.Data.SqlDbType.Float).Value = deviceMsg.WaterTemp;
                            cmd.Parameters.Add("@fill", System.Data.SqlDbType.Bit).Value = deviceMsg.Fill ? 1 : 0;

                            var rows = await cmd.ExecuteNonQueryAsync();
                            log.LogInformation($"{rows} rows were updated");
                        }
                    }
                    catch (Exception e)
                    {
                        // We need to keep processing the rest of the batch - capture this exception and continue.
                        // Also, consider capturing details of the message that failed processing so it can be processed again later.
                        exceptions.Add(e);
                        log.LogError($"Error: {e.Message} {e.StackTrace}");
                    }
                }

                // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

                if (exceptions.Count > 1)
                    throw new AggregateException(exceptions);

                if (exceptions.Count == 1)
                    throw exceptions.Single();
            }
        }
    }
}
