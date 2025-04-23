import sys
import pandas as pd
import requests

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QFileDialog, 
    QTextEdit, QLineEdit, QComboBox, QTableWidget, QTableWidgetItem, QProgressBar
)
from PyQt6.QtGui import QFont
from chardet import detect
from concurrent.futures import ThreadPoolExecutor

API_KEY = "MÓJ KLUCZYK API, KTÓREGO WAM NIE PODAM :)"
MAX_WORKERS = 5

WOJEWODZTWA = [
    "Dolnośląskie", "Kujawsko-Pomorskie", "Lubelskie", "Lubuskie", "Łódzkie",
    "Małopolskie", "Mazowieckie", "Opolskie", "Podkarpackie", "Podlaskie",
    "Pomorskie", "Śląskie", "Świętokrzyskie", "Warmińsko-Mazurskie", "Wielkopolskie", "Zachodniopomorskie"
]

class GeocodingApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.file_path = None
        self.df = None
        self.miasto = ""
        self.wojewodztwo = ""
        self.powiat = ""
        self.ulice = []

    def initUI(self):
        self.setWindowTitle("📍 Geocoding CSV")
        self.setGeometry(100, 100, 900, 700)
        layout = QVBoxLayout()
        font = QFont("Arial", 12)

        self.select_file_btn = QPushButton("📂 Wybierz plik CSV", self)
        self.select_file_btn.setFont(font)
        self.select_file_btn.clicked.connect(self.select_file)
        layout.addWidget(self.select_file_btn)

        self.wojewodztwo_input = QComboBox(self)
        self.wojewodztwo_input.setFont(font)
        self.wojewodztwo_input.addItems(WOJEWODZTWA)
        layout.addWidget(self.wojewodztwo_input)

        self.powiat_input = QLineEdit(self)
        self.powiat_input.setFont(font)
        self.powiat_input.setPlaceholderText("Podaj nazwę powiatu")
        self.powiat_input.textChanged.connect(lambda: self.powiat_input.setText(self.powiat_input.text().upper()))
        layout.addWidget(self.powiat_input)

        self.miasto_input = QLineEdit(self)
        self.miasto_input.setFont(font)
        self.miasto_input.setPlaceholderText("Podaj nazwę miasta")
        self.miasto_input.textChanged.connect(lambda: self.miasto_input.setText(self.miasto_input.text().upper()))
        layout.addWidget(self.miasto_input)

        self.table_widget = QTableWidget(self)
        self.table_widget.setFont(font)
        layout.addWidget(self.table_widget)

        self.clean_btn = QPushButton("✅ Zatwierdź", self)
        self.clean_btn.setFont(font)
        self.clean_btn.clicked.connect(self.clean_inputs)
        layout.addWidget(self.clean_btn)

        self.start_btn = QPushButton("🚀 Rozpocznij Geocoding", self)
        self.start_btn.setFont(font)
        self.start_btn.clicked.connect(self.process_geocoding)
        layout.addWidget(self.start_btn)

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setFont(font)
        layout.addWidget(self.progress_bar)

        self.log_output = QTextEdit(self)
        self.log_output.setFont(font)
        self.log_output.setReadOnly(True)
        layout.addWidget(self.log_output)

        self.setLayout(layout)

    def log(self, message):
        self.log_output.append(message)
        QApplication.processEvents()

    def select_file(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(self, "Wybierz plik CSV", "", "CSV files (*.csv)")
        if file_path:
            self.file_path = file_path
            self.log(f"✅ Wybrano plik: {file_path}")
            self.load_streets()

    def detect_encoding(self, file_path):
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            encoding = detect(raw_data)['encoding']
            return encoding

    def load_streets(self):
        encoding = self.detect_encoding(self.file_path)
        self.log(f"🔍 Wykryte kodowanie: {encoding}, konwersja do utf-8-sig")
        try:
            df = pd.read_csv(self.file_path, encoding=encoding, sep=';')
            df.to_csv(self.file_path, encoding='utf-8-sig', index=False, sep=';')
            self.df = df
            self.ulice = df["STREET"].astype(str) + " " + df["street_number"].astype(str)
            self.display_data(df)
            self.log(f"✅ Załadowano {len(self.ulice)} ulic.")
        except Exception as e:
            self.log(f"❌ Błąd odczytu pliku: {e}")

    def display_data(self, df):
        self.table_widget.setRowCount(df.shape[0])
        self.table_widget.setColumnCount(df.shape[1])
        self.table_widget.setHorizontalHeaderLabels(df.columns)
        for row in range(df.shape[0]):
            for col in range(df.shape[1]):
                self.table_widget.setItem(row, col, QTableWidgetItem(str(df.iat[row, col])))

    def clean_inputs(self):
        self.powiat = ' '.join(self.powiat_input.text().split())
        self.miasto = ' '.join(self.miasto_input.text().split())
        self.powiat_input.setText(self.powiat)
        self.miasto_input.setText(self.miasto)
        self.log("✅ Dane zatwierdzone i oczyszczone z nadmiarowych spacji.")

    def get_coordinates(self, street, street_number, wojewodztwo, miasto, powiat):
        address = f"{street} {street_number}, {miasto}, {powiat}, {wojewodztwo}"
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {'address': address, 'key': API_KEY}
        try:
            res = requests.get(url, params=params, timeout=5).json()
            if res['status'] == 'OK':
                loc = res['results'][0]['geometry']['location']
                powiat_z_api = "Nie znaleziono"
                for component in res['results'][0]['address_components']:
                    if "administrative_area_level_2" in component.get("types", []):
                        powiat_z_api = component.get("long_name")
                        break
                return loc['lat'], loc['lng'], powiat_z_api
            return "Nie znaleziono", "Nie znaleziono", "Nie znaleziono"
        except requests.exceptions.RequestException as e:
            self.log(f"⚠️ Błąd sieci: {address} → {e}")
            return "Nie znaleziono", "Nie znaleziono", "Nie znaleziono"

    def process_geocoding(self):
        if not self.file_path:
            self.log("❌ Brak pliku CSV.")
            return
        self.log("⏳ Geokodowanie w toku...")
        self.progress_bar.setValue(0)
        geocoded_data = []
        total_rows = len(self.df)
        processed_rows = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i, row in self.df.iterrows():
                lat, lng, powiat_z_api = self.get_coordinates(
                    row["STREET"],
                    row["street_number"],
                    self.wojewodztwo_input.currentText(),
                    self.miasto,
                    self.powiat
                )
                geocoded_data.append((
                    row["STREET"],
                    row["street_number"],
                    powiat_z_api,
                    self.wojewodztwo_input.currentText(),
                    lat,
                    lng
                ))
                processed_rows += 1
                self.progress_bar.setValue(int((processed_rows / total_rows) * 100))
        self.save_geocoded_data(geocoded_data)
        self.progress_bar.setValue(100)
        self.log("✅ Geokodowanie zakończone.")

    def save_geocoded_data(self, geocoded_data):
        output_file = self.file_path.replace(".csv", "_geocoded.xlsx")
        df = pd.DataFrame(
            geocoded_data, 
            columns=["STREET", "street_number", "powiat_z_api", "wojewodztwo", "Latitude", "Longitude"]
        )
        try:
            df.to_excel(output_file, index=False, engine='openpyxl')
            self.log(f"✅ Zapisano geokodowane dane do pliku: {output_file}")
        except Exception as e:
            self.log(f"❌ Błąd zapisu pliku: {e}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = GeocodingApp()
    window.show()
    sys.exit(app.exec())
