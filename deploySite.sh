read -s -p "Enter Apache Username: " myusername
echo ""
read -s -p "Enter Apache Password: " mypassword
echo "mvn clean site-deploy scm-publish:publish-scm -Dusername=$myusername -Dpassword=$mypassword -DskipTests $@"
