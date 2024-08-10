""" from base64 import urlsafe_b64decode
from nacl.encoding import URLSafeBase64Encoder
from nacl.secret import SecretBox

decryption_key = "k6zpVFT1IVuiY8j3Avd9cpBVArxhuZ6x0ZG4GmOcF8M="


ssn = "QtgYh_nhLOQCvR9hbdQyiUE0CD3tfhkf.fgEf7NxwBkpq71TE_TiiurzrBPcf4HhP0x2w"


secret_parts = ssn.split(".", 2)
s=secret_parts[0]
nonce = urlsafe_b64decode(s)
ciphertext = urlsafe_b64decode(secret_parts[1])
box = SecretBox(decryption_key, URLSafeBase64Encoder)
ssn_bytes = box.decrypt(ciphertext, nonce)
ssn = ssn_bytes.decode('utf-8')

print(ssn)"""

import pymysql
from base64 import urlsafe_b64decode
from nacl.encoding import URLSafeBase64Encoder
from nacl.secret import SecretBox



def decrypt_and_update_ssn(db_host, db_port, db_user, db_password, db_schema, decryption_key):
    try:
        print("Connecting to the database...")
        conn = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_schema
        )

        cursor = conn.cursor()

				 
        query = ("select individual_id,encrypted_ssn "
        " from individual "
        " where ssn is null and encrypted_ssn is not null "
        " and company_id in (select company_id from invitation where id in "
        " ( SELECT invitation_id from api_connection_status where provider_id='gusto' ))")
		


        # Execute the query
        print("Executing SQL query...")
        cursor.execute(query)

        rows = cursor.fetchall()


        print(f"Number of rows fetched: {len(rows)}")

        # Iterate through the rows
        for row in rows:
            individual_id, encrypted_ssn = row

            secret_parts = encrypted_ssn.split(".", 2)
            s = secret_parts[0]
            nonce = urlsafe_b64decode(s)
            ciphertext = urlsafe_b64decode(secret_parts[1])
            box = SecretBox(decryption_key, URLSafeBase64Encoder)
            ssn_bytes = box.decrypt(ciphertext, nonce)
            decrypted_ssn = ssn_bytes.decode('utf-8')

            # Print decrypted SSN
          #  print(f"Decrypted SSN for individual ID {individual_id}: {decrypted_ssn}")
           # print(row)

            # Update the database with the decrypted SSN
            update_query = "UPDATE individual SET ssn = %s WHERE individual_id = %s"
            cursor.execute(update_query, (decrypted_ssn, individual_id))

        conn.commit()
        print("Decryption and update successful.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()
        conn.close()


decrypt_key = "k6zpVFT1IVuiY8j3Avd9cpBVArxhuZ6x0ZG4GmOcF8M="
decrypt_and_update_ssn("birdprod.csuausqb2ywx.us-east-1.rds.amazonaws.com", 3306, "ladybird", "SunsetInTurk3y!", "birdprod",
                       decrypt_key)


