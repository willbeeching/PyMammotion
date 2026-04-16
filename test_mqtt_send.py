"""Test sending commands via MQTT publish instead of HTTP API.

This bypasses the rate-limited /thing/service/invoke HTTP endpoint
by publishing directly to the MQTT broker.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import ssl
import time
import uuid

import aiomqtt
import aiohttp
from dotenv import load_dotenv

load_dotenv()

from pymammotion.aliyun.cloud_gateway import CloudIOTGateway
from pymammotion.aliyun.exceptions import TooManyRequestsException
from pymammotion.http.http import MammotionHTTP


async def main():
    email = os.environ["MAMMOTION_EMAIL"]
    password = os.environ["MAMMOTION_PASSWORD"]

    async with aiohttp.ClientSession() as session:
        http = MammotionHTTP(session=session)
        print(f"Logging in as {email}...")
        await http.login_v2(email, password)

        gw = CloudIOTGateway(http)
        country_code = http.login_info.userInformation.domainAbbreviation
        await gw.get_region(country_code)
        await gw.connect()
        await gw.login_by_oauth(country_code)
        await gw.aep_handle()
        await gw.session_by_auth_code()
        dev_response = await gw.list_binding_by_account()

        if not dev_response.data or not dev_response.data.data:
            print("No devices found!")
            return

        device = dev_response.data.data[0]
        iot_id = device.iot_id
        print(f"Device: {device.nick_name} (iot_id={iot_id})")

        aep = gw.aep_response.data
        session_data = gw.session_by_authcode_response.data
        region_data = gw.region_response.data

        product_key = aep.productKey
        device_name = aep.deviceName
        device_secret = aep.deviceSecret
        iot_token = session_data.iotToken
        region_id = region_data.regionId

        mqtt_host = f"{product_key}.iot-as-mqtt.{region_id}.aliyuncs.com"
        mqtt_username = f"{device_name}&{product_key}"
        client_id_base = f"{product_key}&{device_name}"

        print(f"\nMQTT host: {mqtt_host}")
        print(f"Product key: {product_key}")
        print(f"Device name: {device_name}")

        # Build MQTT credentials (HMAC-SHA1 signed)
        timestamp = str(int(time.time()))
        client_id = f"{client_id_base}|securemode=2,signmethod=hmacsha1,ext=1,_ss=1,timestamp={timestamp}|"
        sign_content = (
            f"clientId{client_id_base}"
            f"deviceName{device_name}"
            f"productKey{product_key}"
            f"timestamp{timestamp}"
        )
        mqtt_password = hmac.new(
            device_secret.encode("utf-8"),
            sign_content.encode("utf-8"),
            hashlib.sha1,
        ).hexdigest()

        # Topics
        base = f"/sys/{product_key}/{device_name}"
        subscribe_topics = [
            f"{base}/app/down/account/bind_reply",
            f"{base}/app/down/thing/event/property/post_reply",
            f"{base}/app/down/thing/wifi/status/notify",
            f"{base}/app/down/thing/wifi/connect/event/notify",
            f"{base}/app/down/_thing/event/notify",
            f"{base}/app/down/thing/events",
            f"{base}/app/down/thing/status",
            f"{base}/app/down/thing/properties",
            f"{base}/app/down/thing/model/down_raw",
            f"{base}/app/down/thing/service/invoke/reply",
        ]
        publish_topic = f"{base}/app/up/thing/service/invoke"
        bind_topic = f"{base}/app/up/account/bind"

        # Build a get_report_cfg command
        from pymammotion.proto import mctrl_sys_pb2
        msg = mctrl_sys_pb2.MctlSys()
        msg.todev_report_cfg.act = 0
        msg.todev_report_cfg.timeout = 3000
        command = msg.SerializeToString()
        command_b64 = base64.b64encode(command).decode("ascii")

        # TLS context
        from importlib.resources import files as pkg_files
        ca_cert = str(pkg_files("pymammotion.resources").joinpath("ca.pem"))
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        tls_context.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
        tls_context.load_verify_locations(ca_cert)

        print("\nConnecting to MQTT broker...")

        got_response = asyncio.Event()
        response_data = {}

        async with aiomqtt.Client(
            hostname=mqtt_host,
            port=8883,
            username=mqtt_username,
            password=mqtt_password,
            identifier=client_id,
            keepalive=60,
            tls_context=tls_context,
            protocol=aiomqtt.ProtocolVersion.V311,
        ) as client:
            print("Connected! Subscribing to topics...")

            for topic in subscribe_topics:
                await client.subscribe(topic, qos=1)

            # Send bind message
            print("Sending bind message...")
            await client.publish(
                bind_topic,
                json.dumps({
                    "id": "msgid1",
                    "version": "1.0",
                    "request": {"clientId": mqtt_username},
                    "params": {"iotToken": iot_token},
                }),
                qos=1,
            )

            await asyncio.sleep(2)

            # Now send command via MQTT publish
            message_id = str(uuid.uuid4())
            payload = json.dumps({
                "id": message_id,
                "version": "1.0",
                "params": {
                    "iotId": iot_id,
                    "identifier": "device_protobuf_sync_service",
                    "args": {"content": command_b64},
                },
            })

            print(f"\n--- Sending get_report_cfg via MQTT publish ---")
            print(f"Topic: {publish_topic}")
            print(f"Message ID: {message_id}")

            t0 = time.monotonic()
            await client.publish(publish_topic, payload, qos=1)
            print(f"Published! Waiting for response...")

            # Listen for responses for up to 10 seconds
            try:
                async with asyncio.timeout(10):
                    async for message in client.messages:
                        topic = str(message.topic)
                        raw = bytes(message.payload)
                        elapsed = (time.monotonic() - t0) * 1000

                        try:
                            parsed = json.loads(raw)
                            msg_id = parsed.get("id", "")
                            code = parsed.get("code", "")
                            print(f"\n  [{elapsed:.0f}ms] Response on: {topic}")
                            print(f"    id={msg_id}, code={code}")
                            if "params" in parsed:
                                params = parsed["params"]
                                content = None
                                if "value" in params and isinstance(params["value"], dict):
                                    content = params["value"].get("content")
                                elif "content" in params:
                                    content = params["content"]
                                if content:
                                    decoded = base64.b64decode(content)
                                    print(f"    Got protobuf response: {len(decoded)} bytes")
                                    got_response.set()
                                else:
                                    print(f"    Params: {json.dumps(params)[:200]}")
                            else:
                                print(f"    Raw: {raw[:200]}")
                        except (json.JSONDecodeError, ValueError):
                            print(f"\n  [{elapsed:.0f}ms] Non-JSON on: {topic} ({len(raw)} bytes)")

            except TimeoutError:
                elapsed = (time.monotonic() - t0) * 1000
                print(f"\n  Timed out after {elapsed:.0f}ms")

            if got_response.is_set():
                print("\n✓ SUCCESS: Command sent via MQTT and got a response!")
                print("  This means we can bypass the HTTP rate limit entirely.")
            else:
                print("\n✗ No protobuf response received via MQTT publish.")
                print("  The service invoke topic may not be supported for app clients.")

            # Bonus: try sending 5 rapid-fire commands via MQTT
            if got_response.is_set():
                print("\n--- Rapid-fire test: 5 commands, no delay ---")
                for i in range(5):
                    mid = str(uuid.uuid4())
                    p = json.dumps({
                        "id": mid,
                        "version": "1.0",
                        "params": {
                            "iotId": iot_id,
                            "identifier": "device_protobuf_sync_service",
                            "args": {"content": command_b64},
                        },
                    })
                    await client.publish(publish_topic, p, qos=1)
                    print(f"  [{i+1}] Published (id={mid[:8]}...)")

                print("  Listening for responses (10s)...")
                count = 0
                try:
                    async with asyncio.timeout(10):
                        async for message in client.messages:
                            topic = str(message.topic)
                            raw = bytes(message.payload)
                            try:
                                parsed = json.loads(raw)
                                params = parsed.get("params", {})
                                content = None
                                if "value" in params and isinstance(params["value"], dict):
                                    content = params["value"].get("content")
                                elif "content" in params:
                                    content = params["content"]
                                if content:
                                    count += 1
                                    print(f"    Response {count}")
                            except (json.JSONDecodeError, ValueError):
                                pass
                except TimeoutError:
                    pass
                print(f"  Got {count}/5 responses — no HTTP rate limiting!")


if __name__ == "__main__":
    asyncio.run(main())
