VERSION := `head -1 debian/changelog | awk '{print $2}' | grep -o -e '\([0-9\.]\+\)' | tr -d '()'`

pkg:
	dpkg-deb --build debian	


