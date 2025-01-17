// Import the `createClient` function from the `@supabase/supabase-js` library. This function is used to initialize a connection
// to a Supabase project. Supabase is a Backend-as-a-Service (BaaS) platform that provides a PostgreSQL database with built-in
// authentication, storage, and APIs for querying and managing your data. This connection will allow us to interact with the 
// database for reading and writing data.

import { createClient } from '@supabase/supabase-js';

// Define two constants: `SUPABASE_URL` and `SUPABASE_KEY`. These values are essential for connecting to your specific
// Supabase project. The `SUPABASE_URL` is the unique URL endpoint for your Supabase instance, and the `SUPABASE_KEY`
// is the API key required to authenticate your requests. Make sure to secure these keys (especially the Service Role Key)
// and avoid exposing them in public-facing codebases. Here, the `SUPABASE_KEY` is likely a Service Role Key that grants 
// elevated permissions, allowing for operations like inserting or modifying data in the database.

const SUPABASE_URL = 'https://ejuzubdpkfjtvkjmjrda.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVqdXp1YmRwa2ZqdHZram1qcmRhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczNjg3MTczMiwiZXhwIjoyMDUyNDQ3NzMyfQ.mBR2W3DdDycDJpbLHlxxQdQYn4whNVWJt5VTBX12C6Q';

// Initialize the Supabase client using the `createClient` function. This client will act as the interface for all database
// interactions, such as inserting or querying data from specific tables in your Supabase project. With this client object,
// we can call various methods like `.from()` to target specific tables or `.insert()` to add data.

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Export a default object containing an asynchronous `fetch` method. This method is designed to handle the main process
// of fetching data from an external API, transforming it into the appropriate structure, and inserting it into the Supabase database.
// This method will typically be invoked as part of a serverless function or an API endpoint.

export default {
    async fetch(request: Request) {
        // Define an array of station names. Each station represents a specific air quality monitoring location
        // from which we will fetch data. These stations are identified by unique names, such as "EAD_AlAinSchool" 
        // or "EAD_Baniyas". This array acts as the input for our process: we will loop through each station, fetch 
        // its data from the external API, and prepare it for database insertion.

        const stations = [
            "EAD_AlAinSchool", "EAD_AlAinStreet", "EAD_AlMafraq", "EAD_AlMaqta",
            "EAD_AlQuaa", "EAD_AlTawia", "EAD_Baniyas", "EAD_BidaZayed",
            "EAD_E11Road", "EAD_Gayathi", "EAD_Habshan", "EAD_HamdanStreet",
            "EAD_KhadijaSchool", "EAD_KhalifaSchool", "EAD_Liwa", "EAD_Mussafah",
            "EAD_RuwaisTransco", "EAD_Sweihan", "EAD_Zakher", "EAD_KhalifaCity"
        ];

        // Log the list of stations to the console for debugging purposes. This ensures that the process starts correctly
        // and that the array of station names is properly initialized.

        console.log('Starting fetch process for stations:', stations);

        // Define a helper function, `fetchStationData`, which takes a station name as input and retrieves the corresponding
        // air quality data from an external API. This function uses the Fetch API to send an HTTP GET request to the API 
        // endpoint, passing the station name as a query parameter. It then parses the response into JSON format and returns
        // the most recent record for that station. The function is written to handle errors gracefully, returning `null`
        // in case of failure and logging the error for debugging purposes.

        async function fetchStationData(station: string) {
            const API_BASE = "https://www.adairquality.ae/AQAPI/GetHourlyStationChart";
            try {
                console.log(`Fetching data for station: ${station}`);
                const response = await fetch(`${API_BASE}?stationName=${station}`);
                if (!response.ok) throw new Error(`Failed to fetch data for ${station}`);
                const text = await response.text();
                const data = JSON.parse(text);
                console.log(`Data fetched for station ${station}:`, data);
                return data[data.length - 1] || null;
            } catch (error) {
                console.error(`Error fetching data for ${station}:`, error.message);
                return null;
            }
        }

        // Initialize an empty array, `records`, to store the processed data for all stations. As we loop through each
        // station and fetch its data, we will transform the data into a consistent format and append it to this array.
        // This array will ultimately be inserted into the Supabase database.

        const records = [];
        for (const station of stations) {
            // Fetch the air quality data for the current station using the `fetchStationData` function.
            // If valid data is returned, transform it into the required structure and add it to the `records` array.

            const record = await fetchStationData(station);
            if (record) {
                console.log(`Adding record for station ${station}:`, record);
                records.push({
                    sensor_id: station || 'unknown',
                    recorded_date: record.recordedDate
                        ? new Date(record.recordedDate).toISOString().split('T')[0]
                        : null,
                    hour: record.hour || null,
                    pm10: record.pM10 != null ? parseFloat(record.pM10) : null,
                    pm25: record.pM25 != null ? parseFloat(record.pM25) : null,
                    no2: record.nO2 != null ? parseFloat(record.nO2) : null,
                    co: record.co != null ? parseFloat(record.co) : null,
                    o3: record.o3 != null ? parseFloat(record.o3) : null,
                    so2: record.sO2 != null ? parseFloat(record.sO2) : null,
                    aqi: record.aqi != null ? parseFloat(record.aqi) : null,
                    aqiIndex: record.aqiIndex || null,
                });
            } else {
                // Log a warning if no valid data was found for the current station. This helps track any stations
                // where the external API failed to return usable data.
                console.warn(`No valid record found for station ${station}`);
            }
        }

        // Log the final array of records to the console for debugging purposes. This provides a clear view of the
        // data that will be inserted into the database, ensuring everything is structured correctly.

        console.log('Records to be inserted into Supabase:', JSON.stringify(records, null, 2));

        // Attempt to insert the `records` array into the `ead_api_readings` table in the Supabase database.
        // The `from()` method targets the specified table, and the `insert()` method adds the provided data.
        // If the insertion fails, log the error and return a 500 response with the error message.

        try {
            const { data, error } = await supabase.from('ead_api_readings').insert(records);
            if (error) {
                console.error('Supabase Response Error:', JSON.stringify(error, null, 2));
                return new Response(`Error inserting data: ${error.message}`, { status: 500 });
            }

            // Log a success message if the data was successfully inserted into the database, and return a 200 response
            // with a confirmation message.

            console.log('Data successfully inserted into Supabase:', JSON.stringify(data, null, 2));
            return new Response(`Data inserted successfully: ${JSON.stringify(data)}`, { status: 200 });
        } catch (error) {
            // Handle any unexpected errors that occur during the insertion process, logging the error and returning
            // a 500 response with the error message.
            console.error('Unexpected Error:', error.message);
            return new Response(`Unexpected error: ${error.message}`, { status: 500 });
        }
    },
};
