echo "Iniciando criacao de usuarios..."
useradd -m -G users -p '$1$R48kpzsm$wCiNo1pAZlE1jw60RgReo0' user1
setfacl -m u:user1:r-x /home/lisarbjc/lab/lake/GOLD/
setfacl -m u:user1:--- /home/lisarbjc/lab/lake/root
setfacl -m u:user1:--- /home/lisarbjc/lab/lake/user2
setfacl -m u:user1:--- /home/lisarbjc/lab/lake/user3
setfacl -m u:user1:--- /home/lisarbjc/lab/lake/SILVER/
setfacl -m u:user1:--- /home/lisarbjc/lab/lake/BRONZE/
useradd -m -G users -p '$1$.JRFD56.$XJP.1ki7uRr0bzwO/l/aD/' user2
setfacl -m u:user2:r-x /home/lisarbjc/lab/lake/GOLD/
setfacl -m u:user2:r-x /home/lisarbjc/lab/lake/SILVER/
setfacl -m u:user2:--- /home/lisarbjc/lab/lake/root
setfacl -m u:user2:--- /home/lisarbjc/lab/lake/user1
setfacl -m u:user2:--- /home/lisarbjc/lab/lake/user3
setfacl -m u:user2:--- /home/lisarbjc/lab/lake/BRONZE/
useradd -m -G users -p '$1$0ThUdYgP$RDTLvVmrCLiHga3umgS3e.' user3
setfacl -m u:user3:r-x /home/lisarbjc/lab/lake/GOLD/
setfacl -m u:user3:r-x /home/lisarbjc/lab/lake/SILVER/
setfacl -m u:user3:r-x /home/lisarbjc/lab/lake/BRONZE/
setfacl -m u:user3:--- /home/lisarbjc/lab/lake/root
setfacl -m u:user3:--- /home/lisarbjc/lab/lake/user1
setfacl -m u:user3:--- /home/lisarbjc/lab/lake/user2
echo "Finaizada tarefa usuarios!"