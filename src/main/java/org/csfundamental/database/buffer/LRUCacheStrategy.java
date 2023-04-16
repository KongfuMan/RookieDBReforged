package org.csfundamental.database.buffer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LRUCacheStrategy implements CacheStrategy {
    private Map<Long, Tag> pageTagMap;
    private Tag head;
    private Tag tail;
    private int capacity;

    class Tag{
        Tag prev;
        Tag next;
        BufferFrame frame;
        long page;

        private Tag(){
        }

        public Tag(long page, BufferFrame frame){
            if (frame == null){
                throw new IllegalArgumentException("Buffer frame is null");
            }
            this.page = page;
            this.frame = frame;
            this.frame.tag = this;
        }
    }

    public LRUCacheStrategy(int capacity){
        head = new Tag();
        tail = new Tag();
        head.next = tail;
        tail.prev = head;
        pageTagMap = new HashMap<>();
        this.capacity = capacity;
    }

    @Override
    public BufferFrame get(long page) {
        if (!pageTagMap.containsKey(page)){
            return null;
        }

        // move the tag to the tail.
        Tag tag = pageTagMap.get(page);
        removeTag(tag);
        insertToTail(tag);
        return tag.frame;
    }

    @Override
    public BufferFrame put(long page, BufferFrame frame) {
        // cache is full:
        //     if key is in cache: map.remove(key) && remove(old_node) && map.put <key, new_node> && insert new_node to tail
        //     else: map.remove(head.next.key) && remove(head.next) &&  map.put<key, new_node> && insert new_node to tail
        // cache is NOT full:
        //     if key is in cache: map.remove(key) && remove(old_node) && map.put <key, new_node> && insert new_node to tail
        //     else: map.put <key, new_node>  && insert new_node to tail

        // simplify:
        // if page is in cache: map.remove(key) && remove(old_node) && map.put <key, new_node> && insert new_node to tail
        // else:
        //      if cache is full: map.remove(head.next.key) && remove(head.next) &&  map.put<key, new_node> && insert new_node to tail
        //      else: map.put <key, new_node>  && insert new_node to tail

        // simplify further:
        // if page is in cache: map.remove(key) && remove(old_node)
        // else:
        //      if cache is full: map.remove(head.next.key) && remove(head.next)
        // map.put <key, new_node> && insert new_node to tail
        Tag newTag = new Tag(page, frame);
        Tag oldTag = null;
        if (pageTagMap.containsKey(page)){
            remove(page);
        }else{
            if (pageTagMap.size() == capacity){
                // TODO: improve the linear search here
                oldTag = (Tag)evict().tag;
                remove(oldTag.page);
            }
        }
        pageTagMap.put(page, newTag);
        insertToTail(newTag);
        return (oldTag == null) ? null : oldTag.frame;
    }

    @Override
    public BufferFrame evict() {
        Tag tag = head.next;
        while(tag != tail && tag.frame.isPinned()){
            tag = tag.next;
        }
        if (tag == tail){
            throw new IllegalStateException("All frames are pinned.");
        }
        return tag.frame;
    }

    @Override
    public void remove(long page) {
        Tag remove = pageTagMap.remove(page);
        removeTag(remove);
    }

    @Override
    public Iterable<BufferFrame> getAllPageFrames() {
        return pageTagMap.values().stream().map(tag -> tag.frame).collect(Collectors.toList());
    }

    private void removeTag(Tag tag){
        Tag prev = tag.prev;
        Tag next = tag.next;
        prev.next = next;
        next.prev = prev;
    }

    private void insertToTail(Tag tag){
        Tag prevTail = tail.prev;
        prevTail.next = tag;
        tag.prev = prevTail;
        tag.next = tail;
        tail.prev = tag;
    }
}
