THEOS_DEVICE_IP = 192.168.1.13
THEOS_DEVICE_PORT = 22
INSTALL_TARGET_PROCESSES = SpringBoard

ARCHS = arm64

ifdef ROOTFUL
    TARGET := iphone:clang:latest:13.0
    THEOS_PACKAGE_SCHEME =
else ifdef ROOTHIDE
    TARGET := iphone:clang:latest:15.0
    THEOS_PACKAGE_SCHEME = roothide
else
    TARGET := iphone:clang:latest:15.0
    THEOS_PACKAGE_SCHEME = rootless
endif

include $(THEOS)/makefiles/common.mk

TWEAK_NAME = RelayRPCWorker
RelayRPCWorker_FILES = Tweak.x RRWorkerClient.m
RelayRPCWorker_FRAMEWORKS = Foundation
RelayRPCWorker_CFLAGS = -fobjc-arc

include $(THEOS_MAKE_PATH)/tweak.mk

SUBPROJECTS += relayrpcworkerprefs
include $(THEOS_MAKE_PATH)/aggregate.mk
