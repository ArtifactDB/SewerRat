# Accepts the path to the directory of interest,
# plus a comma-separated list of base names.
function registerSewerRat() {
    local mydir=$(realpath $1)
    local payload=$(jq --arg path "${mydir}" '{ "path": $path }' --null-input)

    # Initial request.
    local req=$(curl -s -X POST -L ${SEWER_RAT_URL}/register/start -d "${payload}" -H 'Content-Type: application/json')
    local status=$(echo ${req} | jq -r '.status')
    if [ ${status} == "ERROR" ]; then
        echo "registration failed:" $(echo ${req} | jq -r '.reason') 1>&2
        return 1
    fi

    # Touching the verification file.
    local code=$(echo ${req} | jq -r '.code')
    local verification=${mydir}/${code}
    touch ${verification}

    # Defining the base names of interest.
    local basenames=$2
    local payload=$(jq --arg path "${mydir}" --arg base "${basenames}" '{ "path": $path, "base": $base|split(",") }' --null-input)

    # Confirming the verification.
    local req=$(curl -s -X POST -L ${SEWER_RAT_URL}/register/finish -d "${payload}" -H 'Content-Type: application/json')
    local status=$(echo ${req} | jq -r '.status')
    if [ ${status} == "ERROR" ]; then
        echo "registration failed:" $(echo ${req} | jq -r '.reason') 1>&2
        return 1
    fi

    rm ${verification}
    return 0
}

# Accepts the path to the directory of interest.
function deregisterSewerRat() {
    local mydir=$1
    if [ -e $1 ]
    then
        local mydir=$(realpath $1)
    fi
    local payload=$(jq --arg path "${mydir}" '{ "path": $path }' --null-input)

    # Initial request.
    local req=$(curl -s -X POST -L ${SEWER_RAT_URL}/deregister/start -d "${payload}" -H 'Content-Type: application/json')
    local status=$(echo ${req} | jq -r '.status')
    if [ ${status} == "ERROR" ]; then
        echo "registration failed:" $(echo ${req} | jq -r '.reason') 1>&2
        return 1
    elif [ ${status} == "SUCCESS" ]; then
        return 0
    fi

    # Touching the verification file.
    local code=$(echo ${req} | jq -r '.code')
    local verification=${mydir}/${code}
    touch ${verification}

    # Confirming the verification.
    local req=$(curl -s -X POST -L ${SEWER_RAT_URL}/deregister/finish -d "${payload}" -H 'Content-Type: application/json')
    local status=$(echo ${req} | jq -r '.status')
    if [ ${status} == "ERROR" ]; then
        echo "registration failed:" $(echo ${req} | jq -r '.reason') 1>&2
        return 1
    fi

    rm ${verification}
    return 0
}
