import subprocess
import random
import string

def get_message(N=10):
    return  ''.join(random.choice(string.ascii_uppercase + string.digits + " ") for _ in range(N))


def run():
    N = 50
    flight_numbers = ["1", "2", "3", "4", "5"]


    cmd = """
    gcloud beta pubsub topics publish flight_messages --message '
        {{
        "flight-number" : "{flight}",
        "message" : "{message}",
        "message-type" : "INFO",
        "timestamp" : "2012-04-23T18:25:43.511Z"
        }}
        '
    """

    msg = cmd.format(flight=random.choice(flight_numbers),
                     message=get_message(N))

    subprocess.call(msg, shell=True)

if __name__ == '__main__':
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=50) as executor:
        while True:
            executor.submit(run)