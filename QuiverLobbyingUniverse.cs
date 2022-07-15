/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using NodaTime;
using System.IO;
using QuantConnect.Data;
using System.Collections.Generic;
using System.Globalization;
using static QuantConnect.StringExtensions;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Example custom data type
    /// </summary>
    public class QuiverLobbyingUniverse : BaseData
    {
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);

        /// <summary>
        ///     Full name of the lobbying client
        /// </summary>
        public string Client { get; set; }
        
        /// <summary>
        ///     Category of legislation that is being lobbied for
        /// </summary>
        public string Issue { get; set; }
        
        /// <summary>
        ///     Specific piece of legislation being lobbied for
        /// </summary>
        public string SpecificIssue { get; set; }

        /// <summary>
        /// The Size of spending instance (USD)
        /// </summary>
        public decimal? Amount { get; set; }

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "quiver",
                    "lobbying",
                    "universe",
                    $"{date.ToStringInvariant(DateFormat.EightCharacter)}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(','); 

            var amout = csv[5].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture));

            return new QuiverLobbyingUniverse
            {
                Client = csv[2],
                Issue = csv[3],
                SpecificIssue = csv[4],
                Amount = amout,

                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Time = date.AddDays(-1),    // filename date == EndTime
                Value = amout ?? 0
            };
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return Invariant($"{Symbol}({EndTime}) :: ") +
                Invariant($"Lobbying Client: {Client} ") +
                Invariant($"Lobbying Issue: {Issue} ") +
                Invariant($"Lobbying Amount: {Amount}");
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }
    }
}