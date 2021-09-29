# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Full blog post and notebook here: https://databricks.com/blog/2020/11/20/enforcing-column-level-encryption-and-avoiding-data-duplication-with-pii.html

# COMMAND ----------

# DBTITLE 1,Creating a test table
# MAGIC %sql 
# MAGIC use default; -- Change this value to some other database if you do not want to use the Databricks default
# MAGIC 
# MAGIC drop table if exists Test_Encryption;
# MAGIC 
# MAGIC create table Test_Encryption(Name string, Address string, ssn string);

# COMMAND ----------

# DBTITLE 1,Insert some test records
# MAGIC %sql
# MAGIC insert into Test_Encryption values ('John Doe', 'halloween street, pasadena', '3567812');
# MAGIC insert into Test_Encryption values ('Santa Claus', 'Christmas street, North Pole', '3568123');

# COMMAND ----------

# DBTITLE 1,Create a Fernet key and store it in secrets
# MAGIC %md #### Create a Fernet key and store it in secrets - https://cryptography.io/en/latest/fernet/
# MAGIC 
# MAGIC ##### \*This step needs to be performed in a command line with Databricks CLI installed
# MAGIC 
# MAGIC `pip install databricks-cli`  (Only needed if you do not have the Databricks CLI already installed)
# MAGIC 
# MAGIC `pip install fernet`
# MAGIC 
# MAGIC ##### Generate key using below code in Python
# MAGIC 
# MAGIC `from cryptography.fernet import Fernet`
# MAGIC 
# MAGIC `key = Fernet.generate_key()`
# MAGIC 
# MAGIC ##### Once the key is generated, copy the key value and store it in Databricks secrets
# MAGIC 
# MAGIC  `databricks secrets create-scope --scope encrypt`   <br>
# MAGIC  `databricks secrets put --scope encrypt --key fernetkey`
# MAGIC  
# MAGIC  Paste the key into the text editor, save, and close the program

# COMMAND ----------

# DBTITLE 1,Example to show how Fernet works
# Example code to show how Fernet works and encrypts a text string. DO NOT use the key generated below

from cryptography.fernet import Fernet
# >>> Put this somewhere safe!
key = Fernet.generate_key()
f = Fernet(key)
token = f.encrypt(b"A really secret message. Not for prying eyes.")
print(token)
print(f.decrypt(token))

# COMMAND ----------

# DBTITLE 1,Create Spark UDFs in python for encrypting a value and decrypting a value
# Define Encrypt User Defined Function 
def encrypt_val(clear_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text

# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val
  

# COMMAND ----------

# DBTITLE 1,Use the UDF in a dataframe to encrypt a column
from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType

# Register UDF's
encrypt = udf(encrypt_val, StringType())
decrypt = udf(decrypt_val, StringType())

# Fetch key from secrets
encryptionKey = dbutils.preview.secret.get(scope = "encrypt", key = "fernetkey")

# Encrypt the data 
df = spark.table("Test_Encryption")
encrypted = df.withColumn("ssn", encrypt("ssn",lit(encryptionKey)))
display(encrypted)

#Save encrypted data 
encrypted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Test_Encryption_Table")

# COMMAND ----------

# DBTITLE 1,Decrypt a column in a dataframe
# This is how you can decrypt in a dataframe

decrypted = encrypted.withColumn("ssn", decrypt("ssn",lit(encryptionKey)))
display(decrypted)

# COMMAND ----------

# DBTITLE 1,Using Dynamic Views
# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   current_user as user,
# MAGIC -- Check to see if the current user is a member of the provided group.
# MAGIC   is_member("privileged_pii_users") as privileged

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW v_Test_Encryption AS
# MAGIC SELECT
# MAGIC   name,
# MAGIC   address,
# MAGIC   CASE WHEN
# MAGIC     is_member('privileged_pii_users') THEN ssn
# MAGIC     ELSE 'REDACTED'
# MAGIC   END AS ssn
# MAGIC FROM Test_Encryption;
# MAGIC 
# MAGIC SELECT * FROM v_Test_Encryption
