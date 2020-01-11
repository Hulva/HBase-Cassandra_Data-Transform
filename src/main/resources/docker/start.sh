#!/bin/sh
option=
for arg in "$@"
do
	key=${arg%%=*}
	value=${arg#*=}
	case $key in
	--publicConfig)
		publicConfig=$value
		;;
	--privateConfig)
		privateConfig=$value
		;;
	--local)
		local=$value
		;;
	*)
		option="$option $key=$value"
		;;
	esac
done

if [ ! -n $publicConfig ];then
	echo "public config url must nut null"
	exit 2
fi
if [ ! -n $privateConfig ];then
	echo "private config url must nut null"
	exit 2
fi
if [ ! -n $local ];then
	echo "--local must select e3/e4"
	exit 2
fi

wget -O application.properties $publicConfig
wget -O config.properties $privateConfig

if [ ! -f application.properties ];then
	echo "not find publicConfig application.properties"
	exit 2
fi
if [ ! -f config.properties ];then
	echo "not find privateConfig config.properties"
	exit 2
fi

java -jar shoppingcart-clean.jar  --spring.config.location=config.properties --local=$local $option

