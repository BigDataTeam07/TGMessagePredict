import asyncio
import json
import os
import re
import ssl
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiohttp
import boto3
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from google.cloud import translate_v2 as translate
from google.oauth2 import service_account

# ----------------- configuration ---------------------
CLUSTER_ARN = os.getenv("CLUSTER_ARN")
SECRET_NAME = os.getenv("SECRET_NAME")
SRC_TOPIC = "social-media-topic"
RESULT_TOPIC = "user-sentiment-topic"
GROUP_ID = "bot-processor-group"
BOT_MENTION = "@music_recommender_iss_bot"
WATCH_CHAT_IDS = {-4714765877}
PREDICT_URL = os.getenv("PREDICT_URL")
KEY_PATH = os.getenv("KEY_PATH")
# -----------------------------------------------------


# last 10 messages per user
user_history = defaultdict(lambda: deque(maxlen=10))
# google translate client
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
translator = translate.Client(credentials=credentials)


async def get_msk_connection_info(cluster_arn: str, secret_name: str, region: str = "ap-southeast-1"):
    """
    retrieve MSK SASL authentication information
    """
    sm = boto3.client("secretsmanager", region_name=region)
    secret = sm.get_secret_value(SecretId=secret_name)
    creds = json.loads(secret["SecretString"])
    username = creds["username"]
    password = creds["password"]
    msk = boto3.client("kafka", region_name=region)
    resp = msk.get_bootstrap_brokers(ClusterArn=cluster_arn)
    brokers = resp["BootstrapBrokerStringPublicSaslScram"].split(",")
    return username, password, brokers


async def translate_to_english(text: str) -> str:
    """
    translate non-english text to english
    """
    det = translator.detect_language(text)
    if det.get("language") != "en":
        return translator.translate(text, target_language="en", format_="text").get("translatedText", "")
    return text


def is_same_local_day(ts_ms: int) -> bool:
    """
    whether the given timestamp and local time are on the same day
    """
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone()
    now = datetime.now().astimezone()
    return (dt.year, dt.month, dt.day) == (now.year, now.month, now.day)


async def call_predict(text: str) -> dict:
    """
    call predict service
    """
    en = await translate_to_english(text)
    async with aiohttp.ClientSession() as sess:
        async with sess.post(PREDICT_URL, json={"text": en}, headers={"Content-Type": "application/json"}) as resp:
            resp.raise_for_status()
            print(f"prediction result: {await resp.json()}")
            return await resp.json()


async def preload_history(consumer_params: dict):
    """
    preload history messages to user_history, ignore invalid messages
    """
    cons = AIOKafkaConsumer(SRC_TOPIC, group_id=None, auto_offset_reset="earliest", **consumer_params)
    await cons.start()
    try:
        while True:
            batch = await cons.getmany(timeout_ms=1000)  # no messages in 1s, treat as end of stream
            if not any(batch.values()):
                break
            for tp, msgs in batch.items():
                for msg in msgs:
                    try:
                        rec = json.loads(msg.value.decode("utf-8"))
                    except (json.JSONDecodeError, TypeError):
                        print(f"ignored non-json message at {tp} offset {msg.value}")
                        continue
                    chat_id = rec.get("chat_id")
                    user_id = str(rec.get("user_id"))
                    text = rec.get("message", "")
                    ts_ms = msg.timestamp
                    if (chat_id in WATCH_CHAT_IDS
                            and user_id is not None
                            and isinstance(text, str)
                            and is_same_local_day(ts_ms)):
                        user_history[user_id].append(text)
                        print(f"preloaded history messages: {user_id} -> {text}")
                    else:
                        print(f"ignore invalid message: {rec}")
                        pass
        print("history messages preloaded")
    finally:
        await cons.stop()


async def consume_and_process(consumer_params: dict, producer_params: dict):
    cons = AIOKafkaConsumer(SRC_TOPIC, group_id=GROUP_ID, auto_offset_reset="latest", **consumer_params)
    prod = AIOKafkaProducer(**producer_params)

    await cons.start()
    await prod.start()
    print("Realtime consumer started, waiting for new messages…")
    try:
        async for msg in cons:
            try:
                rec = json.loads(msg.value.decode("utf-8"))
            except (json.JSONDecodeError, TypeError):
                print(f"ignore non-json message at partition {msg.partition} offset {msg.value}")
                continue
            chat_id = rec.get("chat_id")
            user_id = str(rec.get("user_id"))
            text = rec.get("message", "")
            ts_ms = msg.timestamp
            if chat_id not in WATCH_CHAT_IDS or user_id is None or not isinstance(text, str):
                # ignore invalid messages and messages not in watch list
                continue

            # renew user history
            if is_same_local_day(ts_ms):
                user_history[user_id].append(text)

            # trigger prediction
            if BOT_MENTION in text:
                batch = list(user_history[user_id])
                clean_batch = []
                for m in batch:
                    m_clean = re.sub(rf'^{re.escape(BOT_MENTION)}\s*', '', m)
                    clean_batch.append(m_clean)
                print(f"@TryTryWinWin triggered by user={user_id}, clean batch → {clean_batch}")
                try:
                    results = await asyncio.gather(*(call_predict(t) for t in clean_batch), return_exceptions=False)
                except Exception as e:
                    print(f"prediction failed: {e}")
                    continue
                agg = {}
                for r in results:
                    for k, v in r.items():
                        agg[k] = agg.get(k, 0) + v
                agg["user_id"] = user_id
                await prod.send_and_wait(RESULT_TOPIC, json.dumps(agg).encode("utf-8"))
                print(f"Sent aggregated result for user={user_id} to {RESULT_TOPIC}: {agg}")

    finally:
        await cons.stop()
        await prod.stop()


async def main():
    username, password, brokers = await get_msk_connection_info(CLUSTER_ARN, SECRET_NAME)
    ssl_ctx = ssl.create_default_context()
    consumer_params = {
        "bootstrap_servers": brokers,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
        "ssl_context": ssl_ctx,
    }
    producer_params = consumer_params.copy()
    producer_params.update({"client_id": f"producer-{uuid.uuid4().hex}"})

    await preload_history(consumer_params)
    await consume_and_process(consumer_params, producer_params)


if __name__ == "__main__":
    asyncio.run(main())
