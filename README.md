# CDR Generation System

A Python-based system for generating synthetic Call Detail Records (CDR) for Belgian telecommunications data. This system creates realistic telecom data including call records, location information, and device details.

## Features

- Generates 10 million synthetic CDR records
- Realistic Belgian phone numbers and IMEI numbers
- Geographic data within Belgium's boundaries
- Multiple telecom operators with market share distribution
- Batch processing for efficient database insertion
- Automatic retry mechanism for database operations
- Email notifications on completion
- Comprehensive logging system

## Prerequisites

- Python 3.x
- Microsoft SQL Server
- Required Python packages (see `requirements.txt`)

## Environment Setup

1. Create a `.env` file with the following variables:
```env
DB_SERVER=your_server
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER=ODBC Driver 17 for SQL Server
CDR_TABLE=your_table_name
GMAIL_USERNAME=your_gmail
GMAIL_APP_PASSWORD=your_app_password
RECIPIENT_EMAIL=recipient@email.com
```

2. Create a `config.yml` file with the following structure:
```yaml
logging:
  level: INFO
  format: '%(asctime)s - %(levelname)s - %(message)s'
  file: 'cdr_generation.log'

data_types:
  operators:
    - code: "OP1"
      market_share: 40
    - code: "OP2"
      market_share: 30
    - code: "OP3"
      market_share: 30
  call_types: ["VOICE", "SMS", "DATA"]
  call_status: ["SUCCESS", "MISSED", "FAILED"]
  antenna_types: ["MACRO", "MICRO", "PICO"]

ranges:
  duration:
    min: 10
    max: 3600
  antenna_id:
    min: 1
    max: 1000
  altitude:
    min: 0
    max: 100

geography:
  latitude:
    min: 49.5
    max: 51.5
  longitude:
    min: 2.5
    max: 6.5
```

## Installation

1. Clone the repository
2. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Run the CDR generation script:
```bash
python etl/generate_cdr.py
```

The script will:
1. Connect to the specified database
2. Create the CDR table if it doesn't exist
3. Generate and insert records in batches
4. Send an email notification upon completion

## Database Schema

The generated CDR table includes the following fields:
- id (INT, Primary Key)
- caller (VARCHAR)
- callee (VARCHAR)
- start_time (DATETIME)
- duration (INT)
- type (VARCHAR)
- status (VARCHAR)
- imei_caller (VARCHAR)
- imei_receiver (VARCHAR)
- operator_code_emission (VARCHAR)
- antenna_id_emission (VARCHAR)
- antenna_type_emission (VARCHAR)
- antenna_lat_emission (FLOAT)
- antenna_lon_emission (FLOAT)
- antenna_altitude_emission (INT)
- antenna_id_reception (VARCHAR)
- antenna_type_reception (VARCHAR)
- antenna_lat_reception (FLOAT)
- antenna_lon_reception (FLOAT)
- antenna_altitude_reception (INT)
- operator_code_reception (VARCHAR)
- caller_lat_home (FLOAT)
- caller_lon_home (FLOAT)

## Performance Considerations

- Uses batch processing (10,000 records per batch)
- Implements connection retry logic
- Includes database indexes for common query patterns
- Optimized for large-scale data generation

## Logging

Logs are written to both:
- Console output
- File specified in config.yml

## Error Handling

The system includes:
- Database connection retry mechanism
- Batch insertion retry logic
- Comprehensive error logging
- Email notifications for completion

## Contributing

Feel free to submit issues and enhancement requests.

## License

Copyright (c) 2024 Sergembela

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. 