if [ -f /tmp/curl ];
then
	echo "CURL exists"
else
	wget http://files.lancethepants.com/Binaries/curl/curl%207.34.0/curl
	chmod +x curl
fi