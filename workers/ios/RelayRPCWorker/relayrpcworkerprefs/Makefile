ARCHS = arm64
TARGET := iphone:clang:16.5:15.0

include $(THEOS)/makefiles/common.mk

BUNDLE_NAME = relayrpcworkerprefs
relayrpcworkerprefs_FILES = RRWPrefsRootListController.m
relayrpcworkerprefs_INSTALL_PATH = /Library/PreferenceBundles
relayrpcworkerprefs_FRAMEWORKS = UIKit
relayrpcworkerprefs_LDFLAGS = -F$(THEOS)/sdks/iPhoneOS16.5.sdk/System/Library/PrivateFrameworks -framework Preferences
relayrpcworkerprefs_CFLAGS = -fobjc-arc

include $(THEOS_MAKE_PATH)/bundle.mk
