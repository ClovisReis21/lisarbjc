echo "Iniciando atualizacoes..."
apt-get update && apt-get -y upgrade
echo "Atualizacoes finalizadas!"

echo "************************************************************"
echo "Instalando dependencias do projeto..."
echo "************************* Docker ***************************"
apt-get install ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
echo "************************* PySpark ***************************"
apt install python3-pip -y
pip install pyspark findspark
echo "************************** MySql ****************************"
apt install mysql-client-8.0
echo "************************** NodeJs ***************************"
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
apt install -y nodejs
echo "*************************** PM2 *****************************"
npm install pm2@latest -g
