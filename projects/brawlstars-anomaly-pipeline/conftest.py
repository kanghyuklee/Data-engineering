"""pytest 설정 — 프로젝트 루트를 import 경로에 추가(tests/에서 모듈 import 가능)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
