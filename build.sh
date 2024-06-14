echo "Iniciando criacao de imagens..."

docker build ./lab/apps/api-dashboard -t api_dashboard_imagem
docker build ./lab/apps/dashboard -t dashboard_imagem
docker build ./lab/apps/loja_simulada -t loja_simulada_image