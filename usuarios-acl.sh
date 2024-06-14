echo "Iniciando criacao de usuarios..."
adduser user1
adduser user1 users
setfacl -m u:user1:r-x lisarbjc/lab/lake/GOLD/
setfacl -m u:user1:--- lisarbjc/lab/lake/root
setfacl -m u:user1:--- lisarbjc/lab/lake/user2
setfacl -m u:user1:--- lisarbjc/lab/lake/user3
setfacl -m u:user1:--- lisarbjc/lab/lake/SILVER/
setfacl -m u:user1:--- lisarbjc/lab/lake/BRONZE/
adduser user2
adduser user2 users
setfacl -m u:user2:r-x lisarbjc/lab/lake/GOLD/
setfacl -m u:user2:r-x lisarbjc/lab/lake/SILVER/
setfacl -m u:user2:--- lisarbjc/lab/lake/root
setfacl -m u:user2:--- lisarbjc/lab/lake/user1
setfacl -m u:user2:--- lisarbjc/lab/lake/user3
setfacl -m u:user2:--- lisarbjc/lab/lake/BRONZE/
adduser user3
adduser user3 users
setfacl -m u:user3:r-x lisarbjc/lab/lake/GOLD/
setfacl -m u:user3:r-x lisarbjc/lab/lake/SILVER/
setfacl -m u:user3:r-x lisarbjc/lab/lake/BRONZE/
setfacl -m u:user3:--- lisarbjc/lab/lake/root
setfacl -m u:user3:--- lisarbjc/lab/lake/user1
setfacl -m u:user3:--- lisarbjc/lab/lake/user2
echo "Finaizada a tarefa usuarios!"