import sys
import boto3
from datetime import datetime, timedelta


def date_range(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def generate_partition_input_list(start_date, num_of_days, table_location, input_format, output_format, serde_info):
    input_list = []  # Initializing empty list
    end_date = start_date + timedelta(days=num_of_days)  # Getting end date till which partitions needs to be created

    for input_date in date_range(start_date, end_date):
        # # Formatting partition values by padding required zeroes and converting into string
        # year = str(input_date)[0:4].zfill(4)
        # month = str(input_date)[5:7].zfill(2)
        # day = str(input_date)[8:10].zfill(2)

        input_date = str(input_date)  # yyyy-mm-dd format

        for hour in range(24):  # Looping over 24 hours to generate partition input for 24 hours for a day
            hour = str('{:02d}'.format(hour))  # Padding zero to make sure that hour is in two digits

            for minute in range(60):
                minute = str('{:02d}'.format(minute))
                part_location = "{}/{}/{}/{}".format(table_location, input_date, hour, minute)
                input_dict = {
                    'Values': [
                        input_date, hour, minute
                    ],
                    'StorageDescriptor': {
                        'Location': part_location,
                        'InputFormat': input_format,
                        'OutputFormat': output_format,
                        'SerdeInfo': serde_info
                    }
                }
                input_list.append(input_dict.copy())
    return input_list


def break_list_into_chunks(partition_input_list, n):
    for i in range(0, len(partition_input_list), n):
        yield partition_input_list[i:i + n]


if __name__ == '__main__':

    l_args = sys.argv
    if len(l_args) < 5:
        print("All arguments needed: python manage-partitions <database> " 
              "<table> <start date (YYYY-MM-DD format)> <num of days>")
        exit(1)

    l_client = boto3.client('glue')
    l_database = l_args[1]
    l_table = l_args[2]
    l_start_date = datetime.strptime(l_args[3], '%Y-%m-%d').date()
    l_num_of_days = int(l_args[4])

    try:
        response = l_client.get_table(
            DatabaseName=l_database,
            Name=l_table
        )
    except Exception as error:
        print("Exception while fetching table info for {}.{} - {}".format(l_database, l_table, error))
        sys.exit(-1)

    # Parsing table info required to create partitions from table
    l_input_format = response['Table']['StorageDescriptor']['InputFormat']
    l_output_format = response['Table']['StorageDescriptor']['OutputFormat']
    l_table_location = response['Table']['StorageDescriptor']['Location']
    l_serde_info = response['Table']['StorageDescriptor']['SerdeInfo']
    l_partition_keys = response['Table']['PartitionKeys']

    l_partition_input_list = generate_partition_input_list(
        l_start_date,
        l_num_of_days,
        l_table_location,
        l_input_format,
        l_output_format,
        l_serde_info)

    print("Adding {} partitions to the {} table".format(str(len(l_partition_input_list)), l_table))

    l_partition_input_list_chunks = list(break_list_into_chunks(l_partition_input_list, 100))

    for each_input in l_partition_input_list_chunks:
        try:
            create_partition_response = l_client.batch_create_partition(
                DatabaseName=l_database,
                TableName=l_table,
                PartitionInputList=each_input)
        except Exception as error:
            print("Exception while adding partitions into the table {} - {}".format(l_table, error))
            sys.exit(-1)

    print('Partitions added successfully')
