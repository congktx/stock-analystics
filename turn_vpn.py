import asyncio
from datetime import datetime

INTERFACE_NAME = "CloudflareWARP"

async def run_ps(cmd: str):
    process = await asyncio.create_subprocess_exec(
        "powershell", "-Command", cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if stdout:
        print(stdout.decode(errors="ignore").strip())
    if stderr:
        print("ERR:", stderr.decode(errors="ignore").strip())


async def turn_off_vpn():
    print("🔌 Tắt WARP...")
    await run_ps(f'Disable-NetAdapter -Name "{INTERFACE_NAME}" -Confirm:$false')
    await asyncio.sleep(3)

async def turn_on_vpn():
    print("⚙️ Bật lại WARP...")
    await run_ps(f'Enable-NetAdapter -Name "{INTERFACE_NAME}" -Confirm:$false')
    await asyncio.sleep(3)


# async def main():
#     await turn_on_vpn()


# if __name__ == "__main__":
#     asyncio.run(main())
