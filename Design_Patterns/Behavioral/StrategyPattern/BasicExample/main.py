from strategy import StealthStrategy, MagicStrategy, BrawlerStrategy, LordStrategy

rogue = StealthStrategy()
mage = MagicStrategy()
brawler = BrawlerStrategy()
lord = LordStrategy()

class Warrior(object):
    def __init__(self, fighting_style_strategy):
        self._fighting_style_strategy = fighting_style_strategy

    def announce_style(self):
        self._fighting_style_strategy.announce_style()

    def technique_property(self):
        self._fighting_style_strategy.technique_property()

class Brawler(Warrior):
    def __init__(self):
        super(Brawler, self).__init__(brawler)

class Mage(Warrior):
    def __init__(self):
        super(Mage, self).__init__(mage)

class Rogue(Warrior):
    def __init__(self):
        super(Rogue, self).__init__(rogue)

class Lord(Warrior):
    def __init__(self):
        super(Lord, self).__init__(lord)

brawlerObject = Brawler()
brawlerObject.announce_style()
brawlerObject.technique_property()

rogueObject = Rogue()
rogueObject.announce_style()
rogueObject.technique_property()

mageObject = Mage()
mageObject.announce_style()
mageObject.technique_property()

lordObject = Lord()
lordObject.announce_style()
lordObject.technique_property()