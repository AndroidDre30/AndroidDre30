import mysql.connector
from flask import Flask, redirect, url_for, render_template

app = Flask(__name__)

def get_db_connection():
    conn = mysql.connector.connect(
        host='n4k-auto02',
        database='database1',
        user='Shangril@',
        password='bigbird'
    )
    return conn

@app.route('/')
def index():
    return render_template('index.html')
@app.route('/update_invitation', methods=['POST'])
def update_invitation():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE invitation SET individual_step_complete_yn=0, paycheck_step_complete_yn=0, initial_data_pull_dt=NULL WHERE invitation_cd=10451"
    )
    conn.commit()
    cursor.close()
    conn.close()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)