"""Probe Aliyun API rate limits for Mammotion cloud commands.

IMPORTANT: Stop the Mammotion integration in HA before running this,
otherwise both compete for the same rate limit budget.
"""

import asyncio
import os
import time

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
        await gw.session_by_auth_code()
        dev_response = await gw.list_binding_by_account()

        if not dev_response.data or not dev_response.data.data:
            print("No devices found!")
            return

        device = dev_response.data.data[0]
        iot_id = device.iot_id
        print(f"Using device: {device.nick_name} (iot_id={iot_id})")

        from pymammotion.proto import mctrl_sys_pb2
        msg = mctrl_sys_pb2.MctlSys()
        msg.todev_report_cfg.act = 0
        msg.todev_report_cfg.timeout = 3000
        command = msg.SerializeToString()

        print("\nWaiting 30s for rate limit window to reset...")
        await asyncio.sleep(30)

        # Phase 1: find burst limit (rapid fire)
        print("\n--- Phase 1: Burst test (no delay) ---")
        burst_results = []
        for i in range(6):
            t0 = time.monotonic()
            try:
                await gw.send_cloud_command(iot_id, command)
                elapsed = (time.monotonic() - t0) * 1000
                burst_results.append("OK")
                print(f"  [{i+1}] OK ({elapsed:.0f}ms)")
            except TooManyRequestsException:
                elapsed = (time.monotonic() - t0) * 1000
                burst_results.append("429")
                print(f"  [{i+1}] 429 ({elapsed:.0f}ms)")
            except Exception as e:
                elapsed = (time.monotonic() - t0) * 1000
                burst_results.append(f"ERR")
                print(f"  [{i+1}] ERROR: {e} ({elapsed:.0f}ms)")

        burst_ok = sum(1 for r in burst_results if r == "OK")
        print(f"  Burst: {burst_ok}/{len(burst_results)} OK")

        # Phase 2: test specific intervals after cooldown
        print("\nWaiting 60s for full reset...")
        await asyncio.sleep(60)

        print("\n--- Phase 2: Fixed interval tests ---")
        for interval in [5, 10, 15, 20, 30]:
            print(f"\n  Testing {interval}s interval (5 requests):")
            oks = 0
            for i in range(5):
                if i > 0:
                    await asyncio.sleep(interval)
                t0 = time.monotonic()
                try:
                    await gw.send_cloud_command(iot_id, command)
                    elapsed = (time.monotonic() - t0) * 1000
                    oks += 1
                    print(f"    [{i+1}] OK ({elapsed:.0f}ms)")
                except TooManyRequestsException:
                    elapsed = (time.monotonic() - t0) * 1000
                    print(f"    [{i+1}] 429 ({elapsed:.0f}ms)")
                except Exception as e:
                    elapsed = (time.monotonic() - t0) * 1000
                    print(f"    [{i+1}] ERR: {e} ({elapsed:.0f}ms)")
            print(f"  Result: {oks}/5 OK at {interval}s interval")
            if oks == 5:
                print(f"\n  >>> Safe interval found: {interval}s <<<")
                break
            print("  Waiting 60s cooldown before next interval test...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
