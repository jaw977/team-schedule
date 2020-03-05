{
  const _ = this._ || require('lodash');
  const moment = this.moment || require('moment-business-days');

  const att = (name, val = '') => ` ${name}="${_.escape(val)}"`;
  const element = (name) => (content, atts) => {
    if (! _.isObject(atts)) atts = {};
    atts = _.keys(atts).map(name => att(name, atts[name])).join('');
    content = _.concat(content).join('');
    return `<${name}${atts}>${content}</${name}>`;
  }
  const [table, thead, tbody, tr, th, td] = 
    ['table', 'thead', 'tbody', 'tr', 'th', 'td'].map(element);

  class Owner {
    constructor({ id, type, schedule = [] }) {
      this.id = id;
      this.type = type;
      this.schedule = schedule;
    }

    addTask({start = 0, id = '', desc = '', est = 1}) {
      const schedule = this.newSchedule = [...this.schedule];
      if (_.isObject(est)) est = est[this.id] || est[this.type] || 1;
      const task = this.task = { est };
      for (let i = 0; est > 0; i++) {
        const work = schedule[i] || { start: start + est };
        const avail = work.start - start;
        if (avail > 0) {
          const duration = Math.min(est, avail);
          if (! _.has(task, 'start')) task.start = start;
          task.end = start + duration;
          schedule.splice(i, 0, { id, desc, start, end: task.end });
          est -= duration;
        }
        start = Math.max(work.end, start);
      }
    }

    confirmTask() {
      this.schedule = this.newSchedule;
    }
  }

  class TeamSchedule {
    constructor({ startDate, owners, holidays, tasks, formatTaskId = _.identity }) {
      moment.updateLocale('us', { holidays, holidayFormat: 'YYYY-MM-DD' });
      this.startDate = moment(startDate);
      this.owners = _.mapValues(_.keyBy(owners, 'id'), owner => new Owner(owner));
      this.tasks = {};
      if (tasks) this.addTasks(tasks);
      this.formatTaskId = formatTaskId;
    }

    addTasks(tasks) {
      for (const task of tasks) {
        if (_.isString(task.start)) task.start = this.dateToDays(task.start);
        const owners = task.own ? _.concat(task.own).map(id => this.owners[id]) : _.values(this.owners);
        if (task.dep) task.start = this.tasks[task.dep].end;
        for (const owner of owners) owner.addTask(task);
        const owner = _.sortBy(owners, 'task.end')[0];
        owner.confirmTask();
        if (! task.id) continue;
        task.own = owner.id;
        _.assign(task, owner.task);
        this.tasks[task.id] = task;
      }
    }

    end() {
      return _.max(_.map(_.values(this.tasks), 'end'));
    }

    dateToDays(dateString) {
      return moment(dateString).businessDiff(this.startDate);
    }

    daysToDate(days) {
      return this.startDate.businessAdd(days).format('MM/DD ddd');
    }

    taskTable() {
      const header = thead(tr(['Task ID', 'Description', 'Est Days', 'Owner', 'Est Start', 'Est End'].map(th)));
      const rows = _.values(this.tasks).map(task => tr([
        td(this.formatTaskId(task.id)),
        td(_.escape(task.desc) || ''),
        td(task.est.toFixed(1), { class: 'estDays' }),
        td(task.own, { class: 'ownerId' }),
        td(this.daysToDate(task.start)),
        td(this.daysToDate(task.end)),
      ]));
      return table([header, tbody(rows)], { class: 'tasks' });
    }

    ownerTable() {
      const end = this.end();
      const owners = _.values(this.owners);
      const header = thead(tr([th('Date')].concat(owners.map(owner => th(owner.id)))));
      const rows = [];

      for (const owner of owners) {
        const schedule = owner.newSchedule = [...owner.schedule];
        const lastWork = _.last(schedule);
        const ownerEnd = lastWork ? lastWork.end : 0;
        if (ownerEnd < end) owner.newSchedule.push({ start: ownerEnd, end });
      }

      for (let i = 0; i < end; i += 0.5) {
        const cols = i === Math.floor(i) ? [th(this.daysToDate(i), { rowspan: 2, class: 'date' })] : [];
        for (const owner of owners) {
          const work = owner.newSchedule.find( work => work.start === i);
          if (work) cols.push( td(
            this.formatTaskId(work.id) || work.desc,
            { rowspan: 2 * (work.end - work.start), class: work.id && 'taskId' }
          ));
        }
        rows.push(tr(cols));
      }

      return table([header, tbody(rows)], { class: 'owners' });
    }
  }

  if (typeof module != 'undefined') module.exports = TeamSchedule;
  else this.TeamSchedule = TeamSchedule;
}
