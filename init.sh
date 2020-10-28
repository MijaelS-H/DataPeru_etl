sudo apt-get update
sudo apt-get install python3-venv
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt

# Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb

# Chrome driver
wget https://chromedriver.storage.googleapis.com/86.0.4240.22/chromedriver_linux64.zip
unzip chromedriver_linux64.zip

# Clean
rm chromedriver_linux64.zip
rm google-chrome-stable_current_amd64.deb