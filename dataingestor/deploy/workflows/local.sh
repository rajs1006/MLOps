
ENV_PATH='env/local' #(path where feature_store.yml is)
CODE_PATH='src/ireland' #(path where the code is)

printf "is Redis(localhost:6379) already running locally(n,y) : "
read -r SYNC
SYNC=${SYNC:='n'}

if [ "$SYNC" == "n" ]; then
    echo "Starting redis..."
    {
        docker run --rm --detach --name redis -p 6379:6379 redis:latest
    } || {
        echo "Redis server is already running on localhost:6379 !"
    }
fi

printf "Do you want to apply the changes in code?(n,y) : "
read -r APPL
APP=${APPL:='n'}

if [ "$APPL" == "y" ]; then
    echo ""
    echo "Applying feature store..."
    feast --feature-store-yaml ${ENV_PATH}/feature_store.yaml -c ${CODE_PATH} apply
fi

echo ""
echo "Materializing data from DB to Redis..."

printf "Do you want to pull data between 2 dats or just incrementally materialize <range, inc>: "
read -r MAT
MAT=${MAT:='range'}

if [ "$MAT" == "range" ]; then

    printf "Enter start date for materialization, default<2019-03-16>) : "
    read -r STDATE
    STDATE=${STDATE:='2019-03-16'}


    printf "Enter end date for materialization, default<today>) : "
    read -r ENDDATE
    ENDDATE=${ENDDATE:=$(date --date="today" +"%Y-%m-%d")}

    echo ""
    feast --feature-store-yaml ${ENV_PATH}/feature_store.yaml materialize ${STDATE}T00:00:00 ${ENDDATE}T00:00:00
elif [ "$MAT" == "inc" ]; then

    printf "Enter end date for materialization, default<tomorrow>) : "
    read -r ENDDATE
    ENDDATE=${ENDDATE:=$(date --date="tomorrow" +"%Y-%m-%d")}

    echo ""
    feast --feature-store-yaml ${ENV_PATH}/feature_store.yaml materialize-incremental  ${ENDDATE}T00:00:00
fi