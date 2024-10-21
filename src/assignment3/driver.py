from pyspark_assignment.src.assignment3.util import *

spark = spark_session()

log_df = create_df(spark, log_data, log_schema)
log_df.show()

log_df_updated = updateColumnName(log_df)
print('Column names should be log_id, user_id, user_activity, time_stamp: ')
log_df_updated.show()

actions_last_7 = action_performed_last_7(log_df_updated)
print('calculate the number of actions performed by each user in the last 7 days:')
actions_last_7.show()

login_date_df = convert_timestamp_login_date(log_df_updated)
print('Convert the time stamp column to the login_date column with YYYY-MM-DD format:')
login_date_df.show()