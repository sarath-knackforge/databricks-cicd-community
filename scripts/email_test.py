import smtplib
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

sender_email = dbutils.secrets.get("mlops_community", "email_user")
sender_password = dbutils.secrets.get("mlops_community", "email_password")

try:
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender_email, sender_password)
    print("✅ SMTP Login SUCCESS")
    server.quit()
except Exception as e:
    print("❌ SMTP Login FAILED:", e)
