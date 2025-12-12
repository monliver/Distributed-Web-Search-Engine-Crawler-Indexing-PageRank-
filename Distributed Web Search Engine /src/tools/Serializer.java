package tools;

import java.util.*;
import java.net.*;
import java.io.*;

public class Serializer {
  public static byte[] objectToByteArray(Object o) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      // Surface the root cause instead of returning null silently
      throw new RuntimeException("Failed to serialize object of type " + (o == null ? "null" : o.getClass().getName()), e);
    }
  }

  public static Object byteArrayToObject(byte b[], File jarFileToLoadClassesFrom) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      ClassLoader oldCL = Thread.currentThread().getContextClassLoader();
      URLClassLoader newCL = (jarFileToLoadClassesFrom != null) ? new URLClassLoader (new URL[] {jarFileToLoadClassesFrom.toURI().toURL()}, oldCL) : null;
      ObjectInputStream ois = new ObjectInputStream(bais) {
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          try {
            // Try loading from the JAR classloader first
            if (newCL != null) {
              return Class.forName(desc.getName(), false, newCL);
            }
            // Fall back to current thread's classloader
            return Class.forName(desc.getName(), false, oldCL);
          } catch (ClassNotFoundException cnfe) {
            // Last resort: use parent classloader
            return super.resolveClass(desc);
          }
        }
      };
      Object result = ois.readObject();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize object from byte array (size=" + (b == null ? 0 : b.length) + ")", e);
    }
  }
}
