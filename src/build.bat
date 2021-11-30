echo changing directory
cd ..
echo creating build directory
mkdir build
echo copying files to build directory 
copy src build
echo installing dependencies to the build directory
pip install -r src/requirements.txt -t build