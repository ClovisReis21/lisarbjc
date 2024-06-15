echo "Iniciando atualizacoes..."

apt-get update && apt-get -y upgrade
echo "Atualizacoes finalizadas!"
echo "************************************************************"
echo "Instalando dependencias do projeto..."
echo "************************* Docker ***************************"
apt-get install ca-certificates curl -y
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y
echo "************************** MySql ****************************"
apt install mysql-client-8.0 -y
apt install jupyter -y

echo "Atualizacoes concluidas!"