#!/bin/bash

go build mysqldiff.go 
upx -qvf ./mysqldiff

#/bin/cp -f ./bash_autocomplete /etc/bash_completion.d/weition-cli.bash

#PROG=weition-cli
#source /etc/bash_completion.d/weition-cli.bash

echo "打包完成。"
exit 0
