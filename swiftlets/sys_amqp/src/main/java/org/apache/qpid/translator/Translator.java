/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.qpid.translator;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

class Mapping
{
  static Map<String, String> get()
  {
    Map<String, String> temp = new HashMap<String, String>();
    temp.put("JMSCorrelationID", "amqp.correlation_id");
    temp.put("JMSDeliveryMode", "amqp.durable");
    temp.put("JMSDestination", "amqp.to");
    temp.put("JMSExpiration", "amqp.absolute_expiry_time");
    temp.put("JMSMessageID", "amqp.message_id");
    temp.put("JMSPriority", "amqp.priority");
    temp.put("JMSRedelivered", "amqp.delivery_count");
    temp.put("JMSReplyTo", "amqp.reply_to");
    temp.put("JMSTimestamp", "amqp.creation_time");
    temp.put("JMSType", "amqp.annotation");
    return temp;
  }

  static Map<String, String> flip(final Map<String, String> in)
  {
    Map<String, String> out = new HashMap<String, String>();
    for (String k : in.keySet())
    {
      out.put(in.get(k), k);
    }
    return out;
  }
}

enum TokenType
{
  STRING_LITERAL, NUMERIC_LITERAL, WORD, WHITESPACE, OTHER
}

class Tokenizer
{
  private static final char QUOTE = '\'';

  PushbackReader reader;
  TokenType type;
  String token;
  StringBuilder buffer;
  boolean lastCharWasQuote;

  Tokenizer(Reader r)
  {
    reader = new PushbackReader(r);
    buffer = new StringBuilder();
  }

  String getToken()
  {
    return token;
  }

  TokenType getType()
  {
    return type;
  }

  private boolean complete()
  {
    token = buffer.toString();
    buffer = new StringBuilder();
    return false;
  }

  private boolean append(char c)
  {
    buffer.append(c);
    return true;
  }

  private boolean consume(char c)
  {
    switch (type)
    {
      case STRING_LITERAL:
        if (lastCharWasQuote)
        {
          if (c == QUOTE)
          {
            //its an escaped quote, treat as part of literal
            lastCharWasQuote = false;
            return append(c);
          } else
          {
            //its part of the next token, don't consume
            return complete();
          }
        } else
        {
          lastCharWasQuote = (c == QUOTE);
          return append(c);
        }
      case NUMERIC_LITERAL:
        if (Character.isDigit(c) || c == '.')
        {
          return append(c);
        } else
        {
          return complete();
        }
      case WHITESPACE:
        if (Character.isWhitespace(c))
        {
          return append(c);
        } else
        {
          return complete();
        }
      case WORD:
        if (Character.isJavaIdentifierPart(c) || c == '.')
        {
          return append(c);
        } else
        {
          return complete();
        }
      case OTHER:
        return complete();
    }
    return false;
  }

  private void consume() throws IOException
  {
    int c = reader.read();
    if (c < 0)
    {
      complete();
    } else if (!consume((char) c))
    {
      reader.unread(c);
    }
  }

  private void begin(char c)
  {
    if (Character.isDigit(c))
    {
      type = TokenType.NUMERIC_LITERAL;
    } else if (QUOTE == c)
    {
      type = TokenType.STRING_LITERAL;
      lastCharWasQuote = false;
    } else if (Character.isJavaIdentifierStart(c))
    {
      type = TokenType.WORD;
    } else if (Character.isWhitespace(c))
    {
      type = TokenType.WHITESPACE;
    } else
    {
      type = TokenType.OTHER;
    }
    buffer.append(c);
  }

  private boolean begin() throws IOException
  {
    token = null;
    int c = reader.read();
    if (c < 0)
    {
      return false;
    } else
    {
      begin((char) c);
      return true;
    }
  }

  boolean next() throws IOException
  {
    if (begin())
    {
      while (token == null) consume();
      return true;
    } else
    {
      return false;
    }
  }
}

public class Translator
{
  public static final Map<String, String> JMS_TO_AMQP = Mapping.get();
  public static final Map<String, String> AMQP_TO_JMS = Mapping.flip(JMS_TO_AMQP);

  private Tokenizer tokenizer;

  public Translator(Reader input)
  {
    tokenizer = new Tokenizer(input);
  }

  public String translate(final Map<String, String> dictionary) throws java.io.IOException
  {
    StringBuilder output = new StringBuilder();
    while (tokenizer.next())
    {
      String token = tokenizer.getToken();
      switch (tokenizer.getType())
      {
        case WORD:
          String replacement = dictionary.get(token);
          output.append(replacement == null ? token : replacement);
          break;
        default:
          output.append(token);
          break;
      }
    }
    return output.toString();
  }

  public static void main(String[] argv) throws Throwable
  {
//        if (argv.length == 0 || argv[0].equals("--jms-to-amqp")) {
//            System.out.print(new Translator(new InputStreamReader(System.in)).translate(JMS_TO_AMQP));
//        } else if (argv[0].equals("--amqp-to-jms")) {
//            System.out.print(new Translator(new InputStreamReader(System.in)).translate(AMQP_TO_JMS));
//        } else {
//            System.out.println("Specify either --jms-to-amqp or --amqp-to-jms");
//        }
    System.out.println(new Translator(new StringReader("amqp.correlation_id = 'amqp.correlation_id' AND JMSMessageID = '1245' and amqp.durable = PERSISTENT")).translate(AMQP_TO_JMS));
  }
}

