# app/garmin_ui.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Optional

from .adb import AdbClient
from . import ui


@dataclass(frozen=True)
class GarminApp:
    package: str = "com.garmin.android.apps.messenger"
    activity: str = ".activity.MainActivity"

    # UI resource ids we rely on
    input_edit_id: str = "com.garmin.android.apps.messenger:id/newMessageInputEditText"
    send_button_id: str = "com.garmin.android.apps.messenger:id/sendButton"


class GarminUI:
    def __init__(self, adb: AdbClient, app: Optional[GarminApp] = None) -> None:
        self.adb = adb
        self.app = app or GarminApp()

    def _dismiss_overlays(self) -> None:
        # Matches your optional "back back"
        self.adb.input_keyevent(4)
        self.adb.input_keyevent(4)

    def _tap_message_input(self) -> None:
        xml = ui.dump_ui(self.adb)
        node = ui.find_first_by_resource_id(xml, self.app.input_edit_id)
        if not node:
            raise RuntimeError("Could not find message input field (newMessageInputEditText) in UI dump")
        ui.tap_node(self.adb, node)

    def _tap_send(self) -> None:
        xml = ui.dump_ui(self.adb)
        node = ui.find_first_by_resource_id(xml, self.app.send_button_id)
        if not node:
            raise RuntimeError("Could not find send button (sendButton) in UI dump")
        ui.tap_node(self.adb, node)

    @staticmethod
    def _encode_for_adb_input_text(s: str) -> str:
        # Your approach: replace spaces with %s
        return s.replace(" ", "%s")

    def send_to_thread(self, thread_id: int, msg: str, settle_ms: int = 800) -> None:
        """
        Deterministic open of an existing thread by inreach://messageThread/<id>
        and send a message.
        """
        self.adb.ensure_connected()

        self._dismiss_overlays()

        uri = f'inreach://messageThread/{int(thread_id)}'
        self.adb.am_start_view(
            data_uri=uri,
            package=self.app.package,
            activity=self.app.activity,
            flags_hex="0x10000000",
        )
        self.adb.sleep_ms(settle_ms)

        # Tap input
        self._tap_message_input()

        # Type message
        self.adb.input_text(self._encode_for_adb_input_text(msg))
        self.adb.sleep_ms(200)

        # Tap send
        self._tap_send()

    def start_thread_and_send(
        self,
        recipients: Sequence[str],
        msg: str,
        compose_settle_ms: int = 1200,
    ) -> None:
        """
        Open compose (inreach://composedevicemessage/) and create a new conversation.
        Adds one or more recipients, then sends first message.
        """
        if not recipients:
            raise ValueError("recipients must be non-empty")

        self.adb.ensure_connected()

        self._dismiss_overlays()

        # 1) Open compose
        self.adb.am_start_view(
            data_uri="inreach://composedevicemessage/",
            package=self.app.package,
            activity=self.app.activity,
            flags_hex="0x10000000",
        )
        self.adb.sleep_ms(compose_settle_ms)

        # 2) Tap first EditText in Garmin package (To/Recipients field)
        xml = ui.dump_ui(self.adb)
        node = ui.find_first_edittext_in_package(xml, self.app.package)
        if not node:
            raise RuntimeError("Could not find recipients EditText in UI dump")
        ui.tap_node(self.adb, node)

        # 3) Add recipients (type + ENTER, optional TAB/DPAD_CENTER)
        for r in recipients:
            self.adb.input_text(self._encode_for_adb_input_text(r))
            self.adb.input_keyevent(66)  # ENTER commits recipient chip
            self.adb.sleep_ms(300)

            # Optional focus/selection steps (kept because they helped in practice)
            self.adb.input_keyevent(61)  # TAB/NEXT
            self.adb.sleep_ms(200)
            self.adb.input_keyevent(23)  # DPAD_CENTER
            self.adb.sleep_ms(500)

        # 4) Tap message input
        self._tap_message_input()

        # 5) Type msg
        self.adb.input_text(self._encode_for_adb_input_text(msg))
        self.adb.sleep_ms(200)

        # 6) Tap send
        self._tap_send()
