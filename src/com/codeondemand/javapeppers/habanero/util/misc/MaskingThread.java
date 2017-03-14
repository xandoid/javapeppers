package com.codeondemand.javapeppers.habanero.util.misc;


/**
 * This class attempts to erase characters echoed to the console. This
 * code was copied from code found at the following URL.
 * 
 *    http://java.sun.com/developer/technicalArticles/Security/pwordmask/
 */

class MaskingThread extends Thread {
   private volatile boolean stop;
   private char echochar = '*';

  /**
   *@param prompt The prompt displayed to the user
   */
   public MaskingThread(String prompt) {
      System.out.print(prompt);
   }

  /**
   * Begin masking until asked to stop.
   */
   public void run() {

      int priority = Thread.currentThread().getPriority();
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

      try {
         stop = true;
         while(stop) {
           System.out.print("\010" + echochar);
           try {
              // attempt masking at this rate
              Thread.sleep(5);
           }catch (InterruptedException iex) {
              Thread.currentThread().interrupt();
              return;
           }
         }
      } finally { // restore the original priority
         Thread.currentThread().setPriority(priority);
      }
   }

  /**
   * Instruct the thread to stop masking.
   */
   public void stopMasking() {
      this.stop = false;
   }
}