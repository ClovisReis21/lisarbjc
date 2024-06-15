echo "Iniciando criacao de usuarios..."

useradd -m -G users -p '$1$R48kpzsm$wCiNo1pAZlE1jw60RgReo0' user1
setfacl -m u:user1:--x $HOME
setfacl -m u:user1:--x $HOME/lisarbjc
setfacl -m u:user1:--x $HOME/lisarbjc/lab
setfacl -m u:user1:--x $HOME/lisarbjc/lab/lake
setfacl -m u:user1:r-x $HOME/lisarbjc/lab/lake/GOLD
setfacl -m u:user1:rwx $HOME/lisarbjc/lab/lake/user1
useradd -m -G users -p '$1$.JRFD56.$XJP.1ki7uRr0bzwO/l/aD/' user2
setfacl -m u:user2:--x $HOME
setfacl -m u:user2:--x $HOME/lisarbjc
setfacl -m u:user2:--x $HOME/lisarbjc/lab
setfacl -m u:user2:--x $HOME/lisarbjc/lab/lake
setfacl -m u:user2:r-x $HOME/lisarbjc/lab/lake/GOLD
setfacl -m u:user2:r-x $HOME/lisarbjc/lab/lake/SILVER
setfacl -m u:user2:rwx $HOME/lisarbjc/lab/lake/user2
useradd -m -G users -p '$1$0ThUdYgP$RDTLvVmrCLiHga3umgS3e.' user3
setfacl -m u:user3:--x $HOME
setfacl -m u:user3:--x $HOME/lisarbjc
setfacl -m u:user3:--x $HOME/lisarbjc/lab
setfacl -m u:user3:--x $HOME/lisarbjc/lab/lake
setfacl -m u:user3:r-x $HOME/lisarbjc/lab/lake/GOLD
setfacl -m u:user3:r-x $HOME/lisarbjc/lab/lake/SILVER
setfacl -m u:user3:r-x $HOME/lisarbjc/lab/lake/BRONZE
setfacl -m u:user3:rwx $HOME/lisarbjc/lab/lake/user3

echo "Finaizada tarefa usuarios!"
