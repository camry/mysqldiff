#!/bin/bash

go build mysqldiff.go 
upx -qvf ./mysqldiff

/bin/cp -f ./bash_completion/mysqldiff.bash /etc/bash_completion.d/mysqldiff.bash

source /etc/bash_completion.d/mysqldiff.bash

echo "打包完成。"
exit 0
