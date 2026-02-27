from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

try:
    import win32com.client as win32
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pywin32 (win32com). Install with `pip install pywin32`."
    ) from exc

try:
    from pywinauto import Application, Desktop
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pywinauto. Install with `pip install pywinauto`."
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh Marketing Forecast workbook via Smart View/Eessbase ribbon and save."
    )
    parser.add_argument(
        "--workbook",
        default="Marketing Forecast Data.xlsx",
        help="Path to workbook (default: Marketing Forecast Data.xlsx in current folder).",
    )
    parser.add_argument("--username", default="cguyer", help="Essbase username.")
    parser.add_argument(
        "--password",
        default=os.getenv("ESSBASE_PASSWORD"),
        help="Essbase password. If omitted, read from ESSBASE_PASSWORD.",
    )
    parser.add_argument(
        "--dialog-seconds",
        type=int,
        default=120,
        help="Seconds to listen for login dialog after pressing refresh.",
    )
    parser.add_argument(
        "--refresh-timeout",
        type=int,
        default=600,
        help="Max seconds to wait for refresh completion.",
    )
    parser.add_argument(
        "--sheet-name",
        default="2026 Data",
        help="Sheet to activate before refresh (default: 2026 Data).",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Show Excel while running (default is hidden).",
    )
    return parser.parse_args()


def _find_excel_window(hwnd: int, timeout_sec: int = 30):
    end = time.time() + timeout_sec
    last_error = None
    while time.time() < end:
        try:
            window = Desktop(backend="uia").window(handle=hwnd)
            if window.exists(timeout=1):
                return window.wrapper_object()
        except Exception as ex:  # noqa: BLE001
            last_error = ex
        time.sleep(0.5)
    if last_error:
        raise RuntimeError(f"Excel window not found: {last_error}")
    raise RuntimeError("Excel window not found.")


def _find_refresh_button(window):
    preferred_button = None
    preferred_split = None
    fallback_button = None
    fallback_split = None
    for ctrl in window.descendants():
        try:
            text = (ctrl.window_text() or "").strip().lower()
            ctype = ctrl.element_info.control_type
            if text == "refresh" and ctype in ("Button", "SplitButton"):
                if ctrl.is_visible() and ctrl.is_enabled():
                    if ctype == "Button" and preferred_button is None:
                        preferred_button = ctrl
                    elif ctype == "SplitButton" and preferred_split is None:
                        preferred_split = ctrl
                else:
                    if ctype == "Button" and fallback_button is None:
                        fallback_button = ctrl
                    elif ctype == "SplitButton" and fallback_split is None:
                        fallback_split = ctrl
        except Exception:  # noqa: BLE001
            continue
    return preferred_button or preferred_split or fallback_button or fallback_split


def _click_essbase_refresh(hwnd: int) -> None:
    tab_names = ("Smart View", "Essbase")
    deadline = time.time() + 45
    while time.time() < deadline:
        excel_window = _find_excel_window(hwnd, timeout_sec=5)
        tabs = []
        for candidate in excel_window.descendants(control_type="TabItem"):
            try:
                if candidate.window_text() in tab_names and candidate.is_visible() and candidate.is_enabled():
                    tabs.append(candidate)
            except Exception:  # noqa: BLE001
                continue

        for tab in tabs:
            try:
                try:
                    tab.select()
                except Exception:  # noqa: BLE001
                    tab.invoke()
                time.sleep(0.8)
                excel_window = _find_excel_window(hwnd, timeout_sec=5)
                try:
                    excel_window.set_focus()
                except Exception:  # noqa: BLE001
                    pass
                btn = _find_refresh_button(excel_window)
                if btn is not None:
                    try:
                        btn.invoke()
                    except Exception:  # noqa: BLE001
                        btn.click()
                    return
            except Exception:  # noqa: BLE001
                continue
        time.sleep(0.5)
    raise RuntimeError("Could not find/click Smart View or Essbase Refresh button.")


def _try_handle_login_dialog(username: str, password: str) -> bool:
    # First pass: Win32 backend is typically more reliable for Smart View auth dialogs.
    try:
        app = Application(backend="win32").connect(title_re=".*Connect to Data Source.*")
        dlg = app.window(title_re=".*Connect to Data Source.*")
        if dlg.exists(timeout=0.2):
            dlg.set_focus()
            edits = [e for e in dlg.children() if e.friendly_class_name() == "Edit"]
            if len(edits) >= 2:
                try:
                    if not edits[0].window_text().strip():
                        edits[0].set_edit_text(username)
                except Exception:  # noqa: BLE001
                    pass
                edits[1].set_edit_text(password)
            elif len(edits) == 1:
                edits[0].set_edit_text(password)

            clicked = False
            for btn in dlg.children():
                if btn.friendly_class_name() == "Button":
                    text = (btn.window_text() or "").strip().lower()
                    if text in ("connect", "ok", "sign in", "log on", "login"):
                        btn.click()
                        clicked = True
                        break
            if not clicked:
                dlg.type_keys("{ENTER}")
            return True
    except Exception:  # noqa: BLE001
        pass

    # Fallback pass: scan likely auth dialogs with UIA backend.
    try:
        for w in Desktop(backend="uia").windows():
            title = (w.window_text() or "").lower()
            if title.endswith(" - excel"):
                continue
            try:
                dialog = w.wrapper_object()
                edits = dialog.descendants(control_type="Edit")
                if not edits:
                    continue

                labels = " ".join(
                    (x.window_text() or "").lower() for x in dialog.descendants(control_type="Text")
                )
                has_auth_labels = ("user name" in labels and "password" in labels) or ("authentication" in labels)

                connect_btn = None
                for btn in dialog.descendants(control_type="Button"):
                    text = (btn.window_text() or "").strip().lower()
                    if text in ("connect", "ok", "sign in", "log on", "login"):
                        connect_btn = btn
                        break

                if not has_auth_labels and connect_btn is None and "connect to data source" not in title:
                    continue

                dialog.set_focus()
                if len(edits) >= 2:
                    try:
                        if not (edits[0].window_text() or "").strip():
                            edits[0].set_edit_text(username)
                    except Exception:  # noqa: BLE001
                        pass
                    edits[1].set_edit_text(password)
                else:
                    edits[-1].set_edit_text(password)

                if connect_btn is not None:
                    try:
                        connect_btn.invoke()
                    except Exception:  # noqa: BLE001
                        connect_btn.click_input()
                else:
                    dialog.type_keys("{ENTER}")
                return True
            except Exception:  # noqa: BLE001
                continue
    except Exception:  # noqa: BLE001
        return False
    return False


def _auth_dialog_present() -> bool:
    try:
        app = Application(backend="win32").connect(title_re=".*Connect to Data Source.*")
        dlg = app.window(title_re=".*Connect to Data Source.*")
        if dlg.exists(timeout=0.1):
            return True
    except Exception:  # noqa: BLE001
        pass

    try:
        for w in Desktop(backend="uia").windows():
            title = (w.window_text() or "").lower()
            if "connect to data source" in title:
                return True
            if any(k in title for k in ("sign in", "logon", "login")) and not title.endswith(" - excel"):
                return True
    except Exception:  # noqa: BLE001
        return False
    return False


def main() -> int:
    args = parse_args()
    if not args.password:
        raise ValueError("Essbase password is required. Pass --password or set ESSBASE_PASSWORD.")

    workbook_path = Path(args.workbook).expanduser().resolve()
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    excel = None
    workbook = None

    try:
        excel = win32.Dispatch("Excel.Application")
        excel.Visible = bool(args.visible)
        excel.DisplayAlerts = False
        excel.AskToUpdateLinks = False

        workbook = excel.Workbooks.Open(str(workbook_path), 0, False)
        if workbook is None:
            raise RuntimeError(f"Excel opened but workbook handle was null: {workbook_path}")
        time.sleep(2)

        target = None
        for sheet in workbook.Worksheets:
            if sheet.Name == args.sheet_name:
                target = sheet
                break
        if target is None:
            target = workbook.Worksheets.Item(1)
        target.Activate()

        excel_hwnd = int(excel.Hwnd)
        _find_excel_window(excel_hwnd, timeout_sec=10)
        clicked = False
        for _ in range(3):
            try:
                _click_essbase_refresh(excel_hwnd)
                clicked = True
                break
            except RuntimeError:
                time.sleep(1)
        if not clicked:
            raise RuntimeError("Failed to click Smart View/Essbase Refresh button.")

        deadline = time.time() + args.refresh_timeout
        auth_deadline = time.time() + args.dialog_seconds
        handled_auth = False
        earliest_exit = time.time() + 5
        while time.time() < deadline:
            if time.time() < auth_deadline and _try_handle_login_dialog(args.username, args.password):
                handled_auth = True

            auth_open = _auth_dialog_present()
            calc_done = excel.CalculationState == 0
            ready = bool(excel.Ready)
            if (not auth_open) and calc_done and ready and time.time() >= earliest_exit:
                if handled_auth:
                    time.sleep(2)
                break
            time.sleep(1)
        else:
            raise TimeoutError(f"Refresh timed out after {args.refresh_timeout} seconds.")

        workbook.Save()
        print(f"[DONE] Refreshed and saved workbook: {workbook_path}")
        return 0
    finally:
        if workbook is not None:
            workbook.Close(SaveChanges=True)
        if excel is not None:
            excel.Quit()


if __name__ == "__main__":
    raise SystemExit(main())
