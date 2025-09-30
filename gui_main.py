#!/usr/bin/env python3
"""
gui_main.py — Simple GUI wrapper for master_run.py
Requirements: pip install PySide6
"""

from __future__ import annotations
import os, sys, re
from pathlib import Path
from PySide6.QtCore import Qt, QProcess, QSettings, QRegularExpression
from PySide6.QtGui import QIcon, QRegularExpressionValidator
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QFileDialog, QMessageBox,
    QVBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton, QCheckBox,
    QTextEdit, QHBoxLayout, QGroupBox, QProgressBar
)

HERE = Path(__file__).resolve().parent
MASTER = HERE / "master_run.py"

ID_RE = re.compile(r"(?P<a>\d{3})[-_](?P<b>\d{2})[-_](?P<c>\d{3})")

def extract_norm_id(text: str) -> str | None:
    m = ID_RE.search(text or "")
    return f"{m.group('a')}_{m.group('b')}-{m.group('c')}" if m else None

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PEDA Data Proc – Runner")
        self.setMinimumSize(1000, 700)
        self.proc: QProcess | None = None
        self.settings = QSettings("PEDA", "DataProcRunner")

        # ---- Inputs panel
        g = QGridLayout()
        r = 0

        self.caseInput = QLineEdit()
        btnCaseFile = QPushButton("Pick ZIP/Folder…")
        btnCaseFile.clicked.connect(self.pick_case)
        g.addWidget(QLabel("Case (ZIP or folder):"), r, 0); g.addWidget(self.caseInput, r, 1); g.addWidget(btnCaseFile, r, 2); r += 1

        self.mriZip = QLineEdit()
        btnMRI = QPushButton("Pick MRI ZIP…")
        btnMRI.clicked.connect(self.pick_mri_zip)
        g.addWidget(QLabel("MRI ZIP (optional):"), r, 0); g.addWidget(self.mriZip, r, 1); g.addWidget(btnMRI, r, 2); r += 1

        self.mriDir = QLineEdit()
        btnMRIDir = QPushButton("Pick MR DICOM Dir…")
        btnMRIDir.clicked.connect(self.pick_mri_dir)
        g.addWidget(QLabel("MR DICOM Dir (optional):"), r, 0); g.addWidget(self.mriDir, r, 1); g.addWidget(btnMRIDir, r, 2); r += 1

        self.outRoot = QLineEdit()
        btnOut = QPushButton("Pick Out Root…")
        btnOut.clicked.connect(self.pick_out_root)
        g.addWidget(QLabel("Out Root:"), r, 0); g.addWidget(self.outRoot, r, 1); g.addWidget(btnOut, r, 2); r += 1

        self.birthdate = QLineEdit()
        self.birthdate.setPlaceholderText("YYYYMMDD (optional)")
        self.birthdate.setValidator(QRegularExpressionValidator(QRegularExpression(r"^\d{8}$")))
        g.addWidget(QLabel("Patient Birthdate:"), r, 0); g.addWidget(self.birthdate, r, 1); r += 1

        # Flags
        flagsBox = QGroupBox("Options")
        f = QGridLayout()
        self.simPeda = QCheckBox("Simulate PEDA (no MATLAB)")
        self.allowMismatch = QCheckBox("Allow ID mismatch (override strict)")
        self.skipTDC = QCheckBox("Skip TDC")
        self.skipMRI = QCheckBox("Skip MRI")
        self.skipPEDA = QCheckBox("Skip PEDA")
        self.dryRun = QCheckBox("Dry run (don’t execute)")
        f.addWidget(self.simPeda, 0, 0); f.addWidget(self.allowMismatch, 0, 1)
        f.addWidget(self.skipTDC, 1, 0); f.addWidget(self.skipMRI, 1, 1); f.addWidget(self.skipPEDA, 1, 2)
        f.addWidget(self.dryRun, 2, 0)
        flagsBox.setLayout(f)

        # Buttons + progress
        btnRun = QPushButton("Run")
        btnRun.clicked.connect(self.start_run)
        btnStop = QPushButton("Stop")
        btnStop.clicked.connect(self.stop_run)
        self.btnRun, self.btnStop = btnRun, btnStop
        self.btnStop.setEnabled(False)

        self.progress = QProgressBar()
        self.progress.setRange(0, 0) # indefinite
        self.progress.setVisible(False)

        top = QWidget(); topLay = QVBoxLayout(top)
        topLay.addLayout(g); topLay.addWidget(flagsBox)

        # Log console
        self.log = QTextEdit(); self.log.setReadOnly(True); self.log.setLineWrapMode(QTextEdit.NoWrap)
        self.log.setStyleSheet("font-family: Consolas, monospace; font-size: 11pt;")

        # Footer
        foot = QHBoxLayout()
        foot.addWidget(self.progress); foot.addStretch(1)
        foot.addWidget(self.btnRun); foot.addWidget(self.btnStop)

        root = QWidget(); lay = QVBoxLayout(root)
        lay.addWidget(top); lay.addWidget(self.log, 1); lay.addLayout(foot)
        self.setCentralWidget(root)

        self.load_settings()

    # ---- File pickers
    def pick_case(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select TDC ZIP", "", "ZIP Files (*.zip);;All Files (*.*)")
        if not path:
            # maybe folder instead
            folder = QFileDialog.getExistingDirectory(self, "Select Case Folder")
            if folder:
                self.caseInput.setText(folder)
        else:
            self.caseInput.setText(path)
            nid = extract_norm_id(path)
            if nid and not self.outRoot.text():
                # Suggest default out root near the source
                self.outRoot.setText(str(Path(path).resolve().parent))

    def pick_mri_zip(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select MRI ZIP", "", "ZIP Files (*.zip);;All Files (*.*)")
        if path: self.mriZip.setText(path)

    def pick_mri_dir(self):
        path = QFileDialog.getExistingDirectory(self, "Select MR DICOM Directory")
        if path: self.mriDir.setText(path)

    def pick_out_root(self):
        path = QFileDialog.getExistingDirectory(self, "Select Output Root")
        if path: self.outRoot.setText(path)

    # ---- Run
    def build_args(self) -> list[str]:
        if not MASTER.exists():
            QMessageBox.critical(self, "Error", f"master_run.py not found at:\n{MASTER}")
            return []
        case_arg = self.caseInput.text().strip()
        out_root = self.outRoot.text().strip()
        if not case_arg:
            QMessageBox.warning(self, "Missing input", "Please choose a Case ZIP or folder.")
            return []
        if not out_root:
            QMessageBox.warning(self, "Missing output root", "Please choose an Out Root.")
            return []

        args = [str(MASTER), case_arg, "--out-root", out_root]

        # Strict by default; only proceed on mismatch if user checks override
        if self.allowMismatch.isChecked():
            args.append("--allow-id-mismatch")

        # TDC/MRI inputs
        if self.mriZip.text().strip():
            args += ["--mri-input", self.mriZip.text().strip()]
        if self.mriDir.text().strip():
            args += ["--mri-dir", self.mriDir.text().strip()]
        # Birthdate
        bd = self.birthdate.text().strip()
        if bd:
            args += ["--patient-birthdate", bd]
        # Step controls
        if self.simPeda.isChecked(): args.append("--simulate-peda")
        if self.skipTDC.isChecked(): args.append("--skip-tdc")
        if self.skipMRI.isChecked(): args.append("--skip-mri")
        if self.skipPEDA.isChecked(): args.append("--skip-peda")
        if self.dryRun.isChecked(): args.append("--dry-run")

        return [sys.executable] + args

    def start_run(self):
        cmd = self.build_args()
        if not cmd:
            return
        self.save_settings()
        self.log.clear()
        self.append_log(f"$ {' '.join(cmd)}\n\n")
        self.btnRun.setEnabled(False); self.btnStop.setEnabled(True)
        self.progress.setVisible(True)

        self.proc = QProcess(self)
        self.proc.setProgram(cmd[0])
        self.proc.setArguments(cmd[1:])
        self.proc.setWorkingDirectory(str(HERE))
        self.proc.setProcessChannelMode(QProcess.MergedChannels)
        self.proc.readyReadStandardOutput.connect(self.on_stdout)
        self.proc.finished.connect(self.on_finished)
        self.proc.start()

    def stop_run(self):
        if self.proc and self.proc.state() != QProcess.NotRunning:
            self.proc.kill()
        self.btnStop.setEnabled(False)

    def on_stdout(self):
        if not self.proc:
            return
        data = self.proc.readAllStandardOutput().data().decode("utf-8", errors="replace")
        self.append_log(data)

    def on_finished(self, code, status):
        self.append_log("\n")
        self.append_log("=" * 36 + "\n")
        self.append_log(f"GUI: master_run.py finished with code {code}\n")
        self.append_log("=" * 36 + "\n")
        self.btnRun.setEnabled(True); self.btnStop.setEnabled(False)
        self.progress.setVisible(False)

        # Highlight ALERT lines
        if "ALERT:" in self.log.toPlainText():
            QMessageBox.warning(self, "Run completed with alerts", "See highlighted ALERT section in the log.")

    def append_log(self, text: str):
        # Simple highlighting for ALERT and Summary separators
        if "ALERT:" in text:
            self.log.setTextColor(Qt.red)
        elif re.search(r"^={10,}$", text.strip(), re.M):
            self.log.setTextColor(Qt.darkCyan)
        else:
            self.log.setTextColor(Qt.black)
        self.log.moveCursor(self.log.textCursor().End)
        self.log.insertPlainText(text)
        self.log.moveCursor(self.log.textCursor().End)

    # ---- Settings
    def load_settings(self):
        self.caseInput.setText(self.settings.value("caseInput", ""))
        self.mriZip.setText(self.settings.value("mriZip", ""))
        self.mriDir.setText(self.settings.value("mriDir", ""))
        self.outRoot.setText(self.settings.value("outRoot", ""))
        self.birthdate.setText(self.settings.value("birthdate", ""))
        self.simPeda.setChecked(self.settings.value("simPeda", "true") == "true")
        self.allowMismatch.setChecked(self.settings.value("allowMismatch", "false") == "true")
        self.skipTDC.setChecked(self.settings.value("skipTDC", "false") == "true")
        self.skipMRI.setChecked(self.settings.value("skipMRI", "false") == "true")
        self.skipPEDA.setChecked(self.settings.value("skipPEDA", "false") == "true")
        self.dryRun.setChecked(self.settings.value("dryRun", "false") == "true")

    def save_settings(self):
        self.settings.setValue("caseInput", self.caseInput.text())
        self.settings.setValue("mriZip", self.mriZip.text())
        self.settings.setValue("mriDir", self.mriDir.text())
        self.settings.setValue("outRoot", self.outRoot.text())
        self.settings.setValue("birthdate", self.birthdate.text())
        self.settings.setValue("simPeda", "true" if self.simPeda.isChecked() else "false")
        self.settings.setValue("allowMismatch", "true" if self.allowMismatch.isChecked() else "false")
        self.settings.setValue("skipTDC", "true" if self.skipTDC.isChecked() else "false")
        self.settings.setValue("skipMRI", "true" if self.skipMRI.isChecked() else "false")
        self.settings.setValue("skipPEDA", "true" if self.skipPEDA.isChecked() else "false")
        self.settings.setValue("dryRun", "true" if self.dryRun.isChecked() else "false")

def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    if os.name == "nt":
        try: w.setWindowIcon(QIcon(str(HERE / "icon.ico")))
        except Exception: pass
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
