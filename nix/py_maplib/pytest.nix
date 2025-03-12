{
  src,
  buildPythonPackage,
  pytestCheckHook,
  maplib,  
  rdflib
}:

buildPythonPackage {
  pname = "maplib-tests";
  inherit (maplib) version;
  format = "other";

  inherit src;

  # dontUnpack = true;
  dontBuild = true;
  dontInstall = true;

  propagatedBuildInputs = [
    maplib
    rdflib
  ];

  nativeCheckInputs = [
    pytestCheckHook
  ];

  checkPhase = ''
    ls
    cd py_maplib/tests
    pytest
  '';

}
