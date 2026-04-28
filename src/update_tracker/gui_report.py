#!/usr/bin/env python3
import argparse
import datetime

from PySide6.QtCore import Qt, QProcess, QProcessEnvironment
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QCheckBox, QScrollArea, QPushButton, QTextEdit, QGroupBox,
    QTabWidget,
)

from update_tracker import postgres_connect, HostSpec, add_common_args, setup_logging, load_config, build_host_limits
from update_tracker.database import report


class UpdateTrackerWindow(QMainWindow):
    def __init__(self, config: dict, host_limits: dict, show_all: bool = False, dry_run: bool = False,
                 current_ubuntu: str | None = None):
        super().__init__()
        self.config = config
        self.host_limits = host_limits
        self.show_all = show_all
        self.dry_run = dry_run
        self.current_ubuntu = current_ubuntu
        self.server_checkboxes: list[tuple[str, QCheckBox]] = []
        self.active_processes: dict[QProcess, str] = {}
        self.host_output_widgets: dict[str, QTextEdit] = {}
        self.pending_count = 0

        title = "Update Tracker"
        if dry_run:
            title += " [DRY RUN]"
        self.setWindowTitle(title)
        self.resize(800, 700)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        self.header_label = QLabel()
        self.header_label.setWordWrap(True)
        main_layout.addWidget(self.header_label)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs, stretch=2)

        selected_group = QGroupBox("Selected Servers")
        selected_layout = QVBoxLayout(selected_group)
        self.selected_label = QLabel("(none)")
        self.selected_label.setWordWrap(True)
        selected_layout.addWidget(self.selected_label)
        main_layout.addWidget(selected_group)

        self.output_tabs = QTabWidget()
        self.output_tabs.setMinimumHeight(100)
        main_layout.addWidget(self.output_tabs, stretch=1)
        self._set_output_placeholder("Output will appear here when Update is clicked.")

        btn_layout = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.load_report)
        update_label = "Update Selected (dry run)" if dry_run else "Update Selected"
        self.update_btn = QPushButton(update_label)
        self.update_btn.clicked.connect(self.run_update)
        btn_layout.addWidget(self.refresh_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.update_btn)
        main_layout.addLayout(btn_layout)

        self.load_report()

    def _set_output_placeholder(self, text: str):
        self._clear_output_tabs()
        label = QLabel(text)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.output_tabs.addTab(label, "Output")

    def _clear_output_tabs(self):
        while self.output_tabs.count():
            w = self.output_tabs.widget(0)
            self.output_tabs.removeTab(0)
            if w:
                w.deleteLater()

    def load_report(self):
        self.server_checkboxes = []
        while self.tabs.count():
            w = self.tabs.widget(0)
            self.tabs.removeTab(0)
            if w:
                w.deleteLater()
        self.selected_label.setText("(none)")

        conn = postgres_connect(self.config)
        hs = HostSpec(host_limits=self.host_limits)
        issues = report(conn, hs, show_all=self.show_all)
        conn.close()

        current_time = datetime.datetime.now().astimezone()
        a = self.config['ansible']
        c = self.config['cutoffs']
        limits_text = ', '.join(
            f"{inv}: update={c[inv]['update days']}d"
            for inv in a['inventory']
        )
        self.header_label.setText(
            f"<b>UPDATE TRACKER REPORT</b><br>"
            f"Generated: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}<br>"
            f"Limits: {limits_text}"
        )

        self._add_tab("Never Updated", [
            (h, h) for h in sorted(issues.never_updated)
        ])
        self._add_tab("Outdated Updates", [
            (h, f"{h}: last updated {date} ({days} days ago, limit: {self.host_limits.get(h, 0)})")
            for h, date, days in sorted(issues.update_old, key=lambda x: x[2], reverse=True)
        ])
        self._add_tab("Kernel Reboot Needed", [
            (h, h) for h in sorted(issues.kernel_needs_reboot)
        ])
        self._add_tab("Kernel Update Available", [
            (h, h) for h in sorted(issues.kernel_available)
        ])
        old_title = f"Old Ubuntu (< {self.current_ubuntu})" if self.current_ubuntu else "Old Ubuntu Version"
        self._add_tab(old_title, [
            (h, h) for h in sorted(issues.old_version)
        ])

        if self.tabs.count() == 0:
            placeholder = QLabel("All servers are up to date!")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.tabs.addTab(placeholder, "Status")

    def _add_tab(self, title: str, items: list[tuple[str, str]]):
        if not items:
            return
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        for hostname, label_text in items:
            cb = QCheckBox(label_text)
            cb.setChecked(False)
            cb.stateChanged.connect(self._update_selected_panel)
            layout.addWidget(cb)
            self.server_checkboxes.append((hostname, cb))
        scroll.setWidget(widget)
        self.tabs.addTab(scroll, f"{title} ({len(items)})")

    def _update_selected_panel(self):
        selected = sorted({h for h, cb in self.server_checkboxes if cb.isChecked()})
        self.selected_label.setText(', '.join(selected) if selected else "(none)")

    def run_update(self):
        selected = sorted({h for h, cb in self.server_checkboxes if cb.isChecked()})
        if not selected:
            self._set_output_placeholder("No servers selected.")
            return

        update_script = self.config.get('update script', '')
        if not update_script:
            self._set_output_placeholder("ERROR: 'update script' not defined in config.")
            return

        ansible_config = str(self.config['ansible']['config'])
        self._clear_output_tabs()
        self.active_processes = {}
        self.host_output_widgets = {}
        self.pending_count = len(selected)

        for hostname in selected:
            output_edit = QTextEdit()
            output_edit.setReadOnly(True)
            self.output_tabs.addTab(output_edit, hostname)
            self.host_output_widgets[hostname] = output_edit

            args = [hostname + ',', '-m', 'shell', '-a', f'sudo {update_script}']
            if self.dry_run:
                args.append('--check')
            output_edit.append(f"Running: ansible {' '.join(args)}\n")

            process = QProcess(self)
            env = QProcessEnvironment.systemEnvironment()
            env.insert('ANSIBLE_CONFIG', ansible_config)
            process.setProcessEnvironment(env)
            process.readyReadStandardOutput.connect(
                lambda p=process, h=hostname: self._on_host_stdout(p, h)
            )
            process.readyReadStandardError.connect(
                lambda p=process, h=hostname: self._on_host_stderr(p, h)
            )
            process.finished.connect(
                lambda exit_code, _status, h=hostname: self._on_host_finished(exit_code, h)
            )
            self.active_processes[process] = hostname
            process.start('ansible', args)

        self.update_btn.setEnabled(False)
        self.refresh_btn.setEnabled(False)

    def _on_host_stdout(self, process: QProcess, hostname: str):
        data = process.readAllStandardOutput().data().decode('utf-8', errors='replace')
        self.host_output_widgets[hostname].append(data.rstrip())

    def _on_host_stderr(self, process: QProcess, hostname: str):
        data = process.readAllStandardError().data().decode('utf-8', errors='replace')
        self.host_output_widgets[hostname].append(data.rstrip())

    def _on_host_finished(self, exit_code: int, hostname: str):
        self.host_output_widgets[hostname].append(f"\nDone (exit code: {exit_code})")
        self.pending_count -= 1
        if self.pending_count == 0:
            self.update_btn.setEnabled(True)
            self.refresh_btn.setEnabled(True)
            self.load_report()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_common_args(parser)
    parser.add_argument('--all', dest='show_all', action='store_true',
                        help="Include hosts that have a regular update schedule")
    parser.add_argument('--dry-run', action='store_true',
                        help="Pass --check to ansible; show what would run without executing")
    args = parser.parse_args()

    setup_logging(args)
    config = load_config(args)
    host_limits = build_host_limits(config)
    current_ubuntu = str(config['current ubuntu']) if 'current ubuntu' in config else None
    app = QApplication([])
    window = UpdateTrackerWindow(config, host_limits, show_all=args.show_all, dry_run=args.dry_run,
                                 current_ubuntu=current_ubuntu)
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
