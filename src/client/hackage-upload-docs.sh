#!/bin/bash
 
# Options / Usage
# put this script in the same directory as your *.cabal file
# it will use the first line of "cabal info ." to determine the package name
 
# custom options for "cabal install"
CUSTOM_OPTIONS=(--haddock-options='-q aliased')
# hackage server to upload to (and to search uploaded versions for)
HACKAGESERVER=hackage.haskell.org
# put your credentials into ~/.netrc: (see man netrc)
#   machine $HACKAGESERVER
#   login $USERNAME
#   password $PASSWORD
 
# nothing to configure below this line
 
# How it works
#
# It tries to find your package on the given hackage server, and
# uploads the generated -doc.tar.gz.
# It first tries the released version, then the candidate.
#
# To generate the docs it uses "cabal install" to install into a temporary directory,
# with a temporary ghc package db in it.
 
set -e
 
status_code() {
	local code=$(curl "http://${HACKAGESERVER}$1" --silent -o /dev/null --write-out %{http_code})
	echo "http://${HACKAGESERVER}$1 $code" >&2
	echo $code
}
 
self=$(readlink -f "$0")
base=$(dirname "${self}")
cd "${base}"
tmpdir=$(mktemp --tmpdir -d doc-package-XXXXXXX)
trap 'rm -rf "${tmpdir}"' EXIT
 
name=$(cabal info . | head -n1 | awk '{print $2}')
 
if [ "200" = "$(status_code /package/${name})" ]; then
	echo "Found released version ${name}"
	targeturl="/package/${name}/docs"
elif [ "200" = "$(status_code /package/${name}/candidate)" ]; then
	echo "Found candidate version ${name}"
	targeturl="/package/${name}/candidate/docs"
else
	echo "Found no uploaded version"
	targeturl=""
fi
 
 
prefix="${tmpdir}/prefix"
docdir="${prefix}/share/doc/${name}"
packagedb="${tmpdir}/package.conf.d"
mkdir -p "${packagedb}"
pkgdocdir="${tmpdir}/${name}-docs"
pkgdocarchive="${tmpdir}/${name}-doc.tar.gz"
 
cabal configure \
	--builddir="${tmpdir}/dist" \
	--disable-optimization --ghc-option -O0 \
	--docdir="${docdir}" \
	--prefix="${prefix}"
 
# need separate haddock step, as install doesn't forward --builddir to haddock with
#   cabal install --enable-documentation
# otherwise configure+haddock could be merged into install
#   (prefix cabal haddock options with --haddock- for cabal install)
cabal haddock \
	--builddir="${tmpdir}/dist" \
	--html-location='/package/$pkg-$version/docs' \
	--haddock-option='--built-in-themes' \
	"${CUSTOM_OPTIONS[@]}" \
	--contents-location='/package/$pkg-$version' \
	--hyperlink-source

echo "ts 10"
 
cabal install \
	--builddir="${tmpdir}/dist" \
	--docdir="${docdir}" \
	--prefix="${prefix}" \
	--ghc-pkg-option --no-user-package-conf # \
	# --ghc-pkg-option --package-db="${packagedb}"

echo "ts 20"
 
cp -ar "${docdir}/html" "${pkgdocdir}"
(cd "$(dirname ${pkgdocdir})"; tar --format=ustar -caf "${pkgdocarchive}" "$(basename ${pkgdocdir})")
echo "Copying $(basename ${pkgdocdir}) to dist/"
cp -ar "${pkgdocarchive}" dist/
 
if [ "${targeturl}" != "" ]; then
	echo -n "Upload to http://${HACKAGESERVER}${targeturl} (y/N)? "
	read ack
	if [ "${ack}" = "y" -o "${ack}" = "Y" ]; then
		echo "Uploading..."
		curl \
			-X PUT \
			-H "Content-Type: application/x-tar" \
			-H "Content-Encoding: gzip" \
			--data-binary @"${pkgdocarchive}" \
			--digest --netrc \
			"http://${HACKAGESERVER}${targeturl}"
	else
		echo "Not uploading."
	fi
fi
 
echo Done.
