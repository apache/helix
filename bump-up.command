#/bin/bash

# colorful echo
red='\e[00;31m'
green='\e[00;32m'
function cecho
{
  message="$1"
  if [ -n "$message" ]; then
    color="$2"
    if [ -z "$color" ]; then
      echo "$message"
    else
      echo -e "$color$message\e[00m"
    fi
  fi
}

echo There are $# arguments to $0: $*
if [ "$#" -eq 2 ]; then
  version=$1
  new_version=$2
else
  version=`grep -A 1 "<artifactId>helix</artifactId>" pom.xml | grep "<version>" | awk 'BEGIN {FS="[<,>]"};{print $3}'`
  minor_version=`echo $version | cut -d'.' -f3`
  new_minor_version=`expr $minor_version + 1`
  new_version=`echo $version | sed -e "s/${minor_version}/${new_minor_version}/g"`
fi

cecho "bump up: $version -> $new_version" $red

cecho "bump up pom.xml" $green
sed -i "s/${version}/${new_version}/g" pom.xml
# git diff pom.xml
grep -C 1 "$new_version" pom.xml

cecho "bump up helix-core/pom.xml" $green
sed -i "s/${version}/${new_version}/g" helix-core/pom.xml
grep -C 1 "$new_version" helix-core/pom.xml
# git diff helix-core/pom.xml

ivy_file="helix-core-"$version".ivy"
new_ivy_file="helix-core-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-core/$ivy_file ]; then
  cecho "bump up helix-core/$ivy_file" $green
  git mv "helix-core/$ivy_file" "helix-core/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-core/$new_ivy_file"
  grep -C 1 "$new_version" "helix-core/$new_ivy_file"
else
  cecho "helix-core/$ivy_file not exist" $red
fi

cecho "bump up helix-admin-webapp/pom.xml" $green
sed -i "s/${version}/${new_version}/g" helix-admin-webapp/pom.xml
grep -C 1 "$new_version" helix-admin-webapp/pom.xml
# git diff helix-core/pom.xml

ivy_file="helix-admin-webapp-"$version".ivy"
new_ivy_file="helix-admin-webapp-"$new_version".ivy"
# echo "$ivy_file"
if [ -f helix-admin-webapp/$ivy_file ]; then
  cecho "bump up helix-admin-webapp/$ivy_file" $green
  git mv "helix-admin-webapp/$ivy_file" "helix-admin-webapp/$new_ivy_file"
  sed -i "s/${version}/${new_version}/g" "helix-admin-webapp/$new_ivy_file"
  grep -C 1 "$new_version" "helix-admin-webapp/$new_ivy_file"
else
  cecho "helix-admin-webapp/$ivy_file not exist" $red
fi

cecho "bump up mockservice/pom.xml" $green
sed -i "s/${version}/${new_version}/g" mockservice/pom.xml
grep -C 1 "$new_version" mockservice/pom.xml

cecho "bump up helix-core/src/main/resources/cluster-manager-version.properties" $green
sed -i "s/${version}/${new_version}/g" helix-core/src/main/resources/cluster-manager-version.properties
grep -C 1 "$new_version" helix-core/src/main/resources/cluster-manager-version.properties



