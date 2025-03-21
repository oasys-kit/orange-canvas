"""
Test for splashscreen
"""

from datetime import datetime

#import pkg_resources
import importlib_resources, importlib_metadata

from PyQt5.QtGui import QPixmap
from PyQt5.QtCore import Qt, QRect

from ..splashscreen import SplashScreen

from ..test import QAppTestCase
from ... import config


class TestSplashScreen(QAppTestCase):
    def test_splashscreen(self):
        #splash = pkg_resources.resource_filename(
        #             config.__package__,
        #             "icons/orange-splash-screen.png"
        #         )
        ref = importlib_resources.files(__name__).joinpath("icons/orange-splash-screen.png")
        with importlib_resources.as_file(ref) as splash: splash = str(splash)

        w = SplashScreen()
        w.setPixmap(QPixmap(splash))
        w.setTextRect(QRect(100, 100, 400, 50))

        def advance_time():
            now = datetime.now()
            time = now.strftime("%c : %f")
            w.showMessage(time, alignment=Qt.AlignCenter)
            i = now.second % 3
            rect = QRect(100, 100 + i * 20, 400, 50)
            w.setTextRect(rect)
            self.assertEqual(w.textRect(), rect)

            self.singleShot(1, advance_time)

        advance_time()

        w.show()
        self.app.exec_()
