from __future__ import annotations

import logging
import os
import platform
import subprocess


logger = logging.getLogger(__name__)

_WINDOWS_BALLOON_SCRIPT = r"""
& {
    $Title = $env:CODEX_NOTIFY_TITLE
    $Message = $env:CODEX_NOTIFY_MESSAGE
    $DurationSeconds = [int]$env:CODEX_NOTIFY_SECONDS

    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing

    $notify = New-Object System.Windows.Forms.NotifyIcon
    $notify.Icon = [System.Drawing.SystemIcons]::Information
    $notify.BalloonTipIcon = [System.Windows.Forms.ToolTipIcon]::Info
    $notify.BalloonTipTitle = $Title
    $notify.BalloonTipText = $Message
    $notify.Visible = $true
    $notify.ShowBalloonTip([Math]::Max($DurationSeconds, 1) * 1000)
    Start-Sleep -Seconds ([Math]::Max($DurationSeconds, 1) + 1)
    $notify.Dispose()
}
"""


class DesktopNotifier:
    def notify(self, title: str, message: str) -> None:
        raise NotImplementedError


class WindowsDesktopNotifier(DesktopNotifier):
    def __init__(self, duration_seconds: int = 5):
        self._duration_seconds = max(int(duration_seconds), 1)

    def notify(self, title: str, message: str) -> None:
        process_env = os.environ.copy()
        process_env["CODEX_NOTIFY_TITLE"] = title
        process_env["CODEX_NOTIFY_MESSAGE"] = message
        process_env["CODEX_NOTIFY_SECONDS"] = str(self._duration_seconds)
        subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-Command",
                _WINDOWS_BALLOON_SCRIPT,
            ],
            check=True,
            capture_output=True,
            text=True,
            env=process_env,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )


class NoopDesktopNotifier(DesktopNotifier):
    def notify(self, title: str, message: str) -> None:
        logger.info("Desktop notification skipped: %s | %s", title, message)


def build_desktop_notifier(duration_seconds: int) -> DesktopNotifier:
    if platform.system().lower() == "windows":
        return WindowsDesktopNotifier(duration_seconds=duration_seconds)
    logger.warning(
        "Desktop notifications are only implemented for Windows in this project"
    )
    return NoopDesktopNotifier()
