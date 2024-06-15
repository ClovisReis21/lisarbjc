#echo "Iniciando atualizacoes..."

apt-get update && apt-get -y upgrade
echo "Atualizacoes finalizadas!"
echo "************************************************************"
echo "Instalando dependencias do projeto..."
echo "************************** Java ****************************"
apt install openjdk-11-jre-headless -y
echo "************************* Docker ***************************"
apt remove docker docker-engine docker.io containerd runc -y
apt update
apt install apt-transport-https ca-certificates curl gnupg-agent software-properties-common -y
-fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" -y
apt update
apt install docker-ce docker-ce-cli containerd.io -y
systemctl start docker
docker run hello-world
echo "************************** MySql ****************************"
apt install mysql-client-8.0 -y
apt install python3-pip
apt install jupyter -y

echo "Atualizacoes concluidas!"
