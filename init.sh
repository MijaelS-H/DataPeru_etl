sudo apt-get update
sudo apt-get install python3-venv
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt

# Chromedriver
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

sudo apt install ./google-chrome-stable_current_amd64.deb

wget https://chromedriver.storage.googleapis.com/87.0.4280.20/chromedriver_linux64.zip

unzip chromedriver_linux64.zip
