import abc #Python's Abstract Base Class (ABC) library

# goal: to make a set of algorithms, encapsulate each one, then make them interchangable. The point is that this is easy to change / add on / etc., but not easy to mess up.

class FightingStyleStrategyAbstract(object):

    #set class to an abstract class
    __metaclass__ = abc.ABCMeta

    #set method to an abstract method
    @abc.abstractmethod
    def announce_style(self):
        """Required Method"""

    @abc.abstractproperty
    def technique_property(self):
        """Required Property"""

class StealthStrategy(FightingStyleStrategyAbstract):
    def announce_style(self):
        print("Stealth")

    def technique_property(self):
        print("sneaking and stealth")

class MagicStrategy(FightingStyleStrategyAbstract):
    def announce_style(self):
        print("Magic")

    def technique_property(self):
        print("magic and elements")

class BrawlerStrategy(FightingStyleStrategyAbstract):
    def announce_style(self):
        print("Brawler")

    def technique_property(self):
        print("fighting")

class LordStrategy(FightingStyleStrategyAbstract):
    def announce_style(self):
        print("We have people for that. Do not ask us.")

    def technique_property(self):
        print("Daddy's Money")