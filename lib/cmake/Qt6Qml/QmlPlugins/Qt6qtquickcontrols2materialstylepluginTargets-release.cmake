#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Qt6::qtquickcontrols2materialstyleplugin" for configuration "Release"
set_property(TARGET Qt6::qtquickcontrols2materialstyleplugin APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Qt6::qtquickcontrols2materialstyleplugin PROPERTIES
  IMPORTED_COMMON_LANGUAGE_RUNTIME_RELEASE ""
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/qt6/qml/QtQuick/Controls/Material/libqtquickcontrols2materialstyleplugin.so"
  IMPORTED_NO_SONAME_RELEASE "TRUE"
  )

list(APPEND _cmake_import_check_targets Qt6::qtquickcontrols2materialstyleplugin )
list(APPEND _cmake_import_check_files_for_Qt6::qtquickcontrols2materialstyleplugin "${_IMPORT_PREFIX}/lib/qt6/qml/QtQuick/Controls/Material/libqtquickcontrols2materialstyleplugin.so" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
