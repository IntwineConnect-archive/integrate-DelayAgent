from __future__ import absolute_import

from datetime import datetime
from . import settings
import logging
import sys
import json

from volttron.platform.vip.agent import Agent, Core
from volttron.platform.agent import utils
from zmq.log.handlers import TOPIC_DELIM

utils.setup_logging()
_log = logging.getLogger(__name__)

class DelayAgent(Agent):
    
    messageQueue = []
    
    def __init__(self, config_path, **kwargs):
        super(DelayAgent, self).__init__(**kwargs)
        
    @Core.receiver("onstart")
    def starting(self, sender, **kwargs):
        '''
        Subscribes to the platform message bus on 
        the CTAevent TOPIC_DELIM
        '''
        self.vip.pubsub.subscribe('pubsub','CTAevent', callback = self.scheduleNewMessage)
        
    def scheduleNewMessage(self, peer, sender, bus, topic, headers, message):
        '''callback for CTAevent topic match'''
        mesdict = json.loads(message)
        deliveryTime = mesdict.get('ADR_start_time', 'now')
        
        #if the message is meant to be acted on immediately, just let the ucmagent handle it
        if deliveryTime == 'now':
            if settings.DEBUGGING_LEVEL >= 1:
                print("Delay Agent ignored a message meant for immediate action")
            return 0   
        #otherwise, put this message on the queue 
        mesdict["ADR_start_time"] = "now"
        procmes = json.dumps(mesdict)
            
        now = datetime.utcnow()
        schedTime = datetime.strptime(deliveryTime,"%Y-%m-%dT%H:%M:%S.%fZ")
        
        if now > schedTime:
            #message is supposed to have been sent already
            #should it be scrapped...
            if settings.DEBUGGING_LEVEL >= 1:
                print("Delay Agent found a new message that should already have been sent")
            return 0
            #or better late than never?
        
        print("Delay Agent parsed message to to be delivered at {time}".format(time = deliveryTime))
            
        #add message to queue
        mestuple = ( schedTime, procmes)
        self.messageQueue.append(mestuple)   
        #sort message list
        self.messageQueue.sort()
        
        return 1
        
        
    @Core.periodic(settings.CHECK_SCHEDULE_INTERVAL)
    def CheckSchedule(self):
        '''see if any previously logged messages are now to be sent and send them'''
        now = datetime.utcnow()
        
        if settings.DEBUGGING_LEVEL >= 2:
            print("checking schedule for pending messages")
        
        
        for qindex in self.messageQueue:
            if qindex[0] < now:
                print("Delay Agent republishing message {msg} \n CONTINUED: scheduled for delivery at {time}".format(msg = qindex[1], time = qindex[0].isoformat() + 'Z'))
                self.vip.pubsub.publish('pubsub', 'CTAevent', {}, qindex[1])
                #remove the message from the queue once it has been sent
                self.messageQueue.remove(qindex)
            
        return 1 
        
def main(argv = sys.argv):
    '''Main method called by the eggsecutable.'''
    try:
        utils.vip_main(DelayAgent)
    except Exception as e:
        _log.exception('unhandled error')
        
if __name__ == '__main__':
    #Entry point for script
    sys.exit(main())
        